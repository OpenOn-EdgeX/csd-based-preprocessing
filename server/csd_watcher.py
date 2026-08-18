#!/usr/bin/env python3
"""CSD Data Watcher (구 csd_server.py).

CSD 스토리지 파티션을 감시하며 엣지 디바이스 데이터 도착을 감지한다.
**전처리를 실행하지 않는다** — 전처리는 분산 경로(PreprocessingWorkload →
PreprocessingJob → CPU/CSD 워커 → 라벨 게이트 → stage2)가 수행한다.
그래서 이름이 server 가 아니라 watcher 다. (--legacy-inprocess 만 예외)

감지 이후의 동작은 세 가지 모드가 있다:

  watch 모드 (기본, k8s 운영)
    데이터 도착을 감지해 기록만 한다. 전처리는 하지 않는다.
    **PreprocessingWorkload 는 스케줄러만 생성한다** — 워크로드 생산자를 하나로
    두어 같은 입력에 대한 중복 제출/중복 처리를 원천 차단한다.

  workload 모드 (--emit-workload)
    감지한 배치를 PreprocessingWorkload CR 로 직접 제출한다(자동 인입).
    스케줄러 없이 "파일 도착 → 분산 전처리" 를 자동화하고 싶을 때 켠다.
    스케줄러와 동시에 켜면 같은 inputPath 에 pw 가 둘 생길 수 있으므로,
    생산자는 둘 중 하나만 두는 것을 전제로 한다.

  legacy 모드 (--legacy-inprocess, CSD 단독 데모)
    예전처럼 이 프로세스 안에서 PreprocessingEngine 으로 Stage 1/2 를 직접
    실행한다(cleaned/ → preprocessed/). k8s 없이 CSD 하나로 도는 데모용.

    ★ 주의 — 이 모드는 "실행 경로 이원화"를 되살린다.
      운영 경로(pw → pj → 워커)와 이 모드는 같은 raw_data 를 각자 처리해서
      서로 다른 위치에 결과를 낸다(pj_out/<이름>/ vs cleaned/·preprocessed/).
      동시에 켜면 이런 문제가 생긴다:
        - 같은 데이터가 두 번 처리되고, 같은 노드 CPU 를 두고 경합한다
          → 분산 경로의 처리량/지연(KPI) 측정치가 오염된다
        - 이 모드는 샤딩도 CSD 오프로드도 하지 않아 CSD 가속 효과가 없다
        - 스케줄러/네임스페이스 쿼터/디바이스 플러그인 밖에서 자원을 쓴다
        - 학습이 어느 data.yaml 을 봐야 하는지 모호해진다
      그래서 k8s 운영 중에는 켜지 않는다. k8s 없이 CSD 단독으로 시연할 때만 쓴다.

사용법:
  k8s 운영:  python server/csd_watcher.py --base-dir /storage/csd_preprocessing
  자동 인입:  ... --emit-workload --host-base-dir /home/ngd/storage/csd_preprocessing
  단독 데모:
    Terminal 1 (CSD):  python server/csd_watcher.py --legacy-inprocess \\
                           --base-dir /home/ngd/storage/csd_preprocessing
    Terminal 2 (HOST): ./server/copy_data.sh
    Terminal 3 (HOST): ./server/trigger_stage2.sh
"""

import ctypes.util
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# Project root
PROJECT_ROOT = Path(__file__).parent.parent
STORAGE_ROOT = Path("/home/ngd/storage")

# Auto-detect CSD environment and set paths
sys.path.insert(0, str(PROJECT_ROOT))
_pylibs = STORAGE_ROOT / "pylibs"
if _pylibs.exists():
    sys.path.insert(0, str(_pylibs))
_opencv_libs = STORAGE_ROOT / "opencv-libs"
if _opencv_libs.exists():
    os.environ["LD_LIBRARY_PATH"] = str(_opencv_libs) + ":" + os.environ.get("LD_LIBRARY_PATH", "")
    for so in sorted(_opencv_libs.glob("*.so.*")):
        try:
            ctypes.cdll.LoadLibrary(str(so))
        except OSError:
            pass

from csd_preprocessor.core.engine import PreprocessingEngine
from csd_preprocessor.core.job import Job
from csd_preprocessor.core.status import StatusTracker
from typing import List, Set, Union

# Suppress library logs, use our own formatting
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("csd_watcher")
logger.setLevel(logging.DEBUG)

# Constants
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".webp"}
POLL_INTERVAL = 2.0
DEBOUNCE_SECONDS = 3.0

# PreprocessingWorkload 제출용 (workload 모드)
GROUP, VERSION, WL_PLURAL = "edgeai.keti.re.kr", "v1alpha1", "preprocessingworkloads"
ACTIVE_PHASES = ("", "Pending", "Planning", "Dispatched", "Running")


class WorkloadEmitter:
    """감지한 배치를 PreprocessingWorkload CR 로 제출한다.

    실행 방식(파이프라인 템플릿, 분할 알고리즘, 라벨 게이트)은 전부 매니저·실행엔진
    쪽 정책이므로 여기서는 "무엇을(입력 경로) 처리하라"만 선언한다.

    경로 주의: 이 감시자는 컨테이너 경로(/storage/...)를 보지만 워커 Job 은 노드의
    hostPath 를 마운트하므로, CR 에는 **노드 경로**(--host-base-dir)를 넣어야 한다."""

    def __init__(self, namespace: str, host_base: Path, name_prefix: str = "auto"):
        from kubernetes import client, config

        kubeconfig = os.environ.get("KUBECONFIG", "")
        if kubeconfig and Path(kubeconfig).exists():
            config.load_kube_config(kubeconfig)
        else:
            config.load_incluster_config()
        self.crd = client.CustomObjectsApi()
        self.namespace = namespace
        self.host_base = Path(host_base)
        self.prefix = name_prefix
        self.input_path = str(self.host_base / "raw_data" / "images")

    def _list(self) -> list:
        return self.crd.list_namespaced_custom_object(
            GROUP, VERSION, self.namespace, WL_PLURAL).get("items", [])

    def active_workload(self):
        """같은 입력을 처리 중인 워크로드가 있으면 그 이름 (중복 제출 방지)."""
        for wl in self._list():
            ds = ((wl.get("spec") or {}).get("workload") or {}).get("dataset") or {}
            if ds.get("inputPath") != self.input_path:
                continue
            if (wl.get("status") or {}).get("phase", "") in ACTIVE_PHASES:
                return wl["metadata"]["name"]
        return None

    def submit(self, total_samples: int) -> str:
        name = f"{self.prefix}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        body = {
            "apiVersion": f"{GROUP}/{VERSION}",
            "kind": "PreprocessingWorkload",
            "metadata": {
                "name": name,
                "namespace": self.namespace,
                "labels": {"part": "preprocess-csd", "created-by": "csd-watcher"},
            },
            "spec": {
                "workload": {
                    "dataset": {
                        "inputPath": self.input_path,
                        "outputPath": str(self.host_base / "pj_out" / name),
                        "totalSamples": total_samples,
                    },
                    # 파이프라인 템플릿/라벨 게이트/분할 알고리즘은 매니저 기본값을 따른다
                    # (labelPath 미지정 → <inputPath>/../annotations 를 게이트가 감시)
                },
            },
        }
        self.crd.create_namespaced_custom_object(GROUP, VERSION, self.namespace, WL_PLURAL, body)
        return name

    def bridge_label_path(self, label_path: str) -> list:
        """trigger.json 의 label_path 를 라벨 대기 중인 워크로드에 반영 (호환 브리지).

        HOST 의 기존 trigger_stage2.sh 가 관례 밖 경로로 어노테이션을 올린 경우에도
        stage2 가 진행되도록, 대기 중인 PreprocessingJob 의 감시 경로를 바꿔준다."""
        patched = []
        pjs = self.crd.list_namespaced_custom_object(
            GROUP, VERSION, self.namespace, "preprocessingjobs").get("items", [])
        for pj in pjs:
            if (pj.get("status") or {}).get("stage") != "waiting_labels":
                continue
            if (pj.get("spec") or {}).get("label_dataset_path") == label_path:
                continue
            self.crd.patch_namespaced_custom_object(
                GROUP, VERSION, self.namespace, "preprocessingjobs", pj["metadata"]["name"],
                {"spec": {"label_dataset_path": label_path}})
            patched.append(pj["metadata"]["name"])
        return patched


def timestamp():
    return datetime.now().strftime("%H:%M:%S")


def print_banner(base_dir: Path, legacy: bool = False):
    print()
    print("+" + "=" * 60 + "+")
    title = "CSD Preprocessing Engine (legacy)" if legacy else "CSD Data Watcher"
    print(f"|  {title:<58}|")
    print("|  Computational Storage Device - nvme2n1p3                |")
    print("+" + "-" * 60 + "+")
    print(f"|  Watch:  {str(base_dir / 'raw_data/'):<50}|")
    if legacy:
        # 인프로세스 실행 모드에서만 이 디렉터리에 결과가 쌓인다
        print(f"|  Stage1: {str(base_dir / 'cleaned/'):<50}|")
        print(f"|  Stage2: {str(base_dir / 'preprocessed/'):<50}|")
    print("+" + "=" * 60 + "+")
    print()


def print_stage_result(stage_name: str, metrics: dict, total: int):
    """Print a single stage result with progress bar."""
    bar_width = 20
    filled = bar_width  # completed = full bar
    bar = "#" * filled + "-" * (bar_width - filled)

    detail = ""
    if stage_name == "validate":
        inv = metrics.get("invalid", 0)
        detail = f"(invalid={inv})" if inv > 0 else ""
    elif stage_name == "deduplicate":
        dup = metrics.get("duplicates", 0)
        detail = f"({dup} duplicates removed)" if dup > 0 else "(0 duplicates)"
    elif stage_name == "filter_quality":
        flt = metrics.get("filtered", 0)
        detail = f"({flt} filtered)" if flt > 0 else "(0 filtered)"
    elif stage_name == "resize":
        ts = metrics.get("target_size", [640, 640])
        detail = f"(-> {ts[0]}x{ts[1]})"
    elif stage_name == "normalize":
        mean = metrics.get("mean", [])
        if mean:
            detail = f"(mean={[round(x, 2) for x in mean]})"
    elif stage_name == "convert_annotation":
        cls = metrics.get("classes", 0)
        src = metrics.get("source_format", "")
        detail = f"({src}->YOLO, {cls} classes)"
    elif stage_name == "augment":
        methods = metrics.get("methods", [])
        aug = metrics.get("augmented", 0)
        detail = f"({'+'.join(methods)}, {aug} generated)"
    elif stage_name == "split":
        splits = metrics.get("splits", {})
        parts = [f"{k}={v}" for k, v in splits.items()]
        detail = f"({'/'.join(parts)})"
    elif stage_name == "statistics":
        cd = metrics.get("class_distribution", {})
        bbox = cd.get("total_bounding_boxes", 0)
        avg = cd.get("avg_bboxes_per_image", 0)
        if bbox:
            detail = f"({bbox} bboxes, avg {avg}/img)"

    processed = metrics.get("total", metrics.get("total_scanned", total))
    print(f"           {stage_name:<18} [{bar}] {processed:>3}/{total:<3} {detail}")


class CSDDataWatcher:
    """CSD 스토리지 파티션 감시자.

    raw_data/ 에 새 이미지가 도착하면 감지하고, 모드에 따라
    기록만 하거나(watch) 워크로드를 제출하거나(workload)
    직접 처리한다(legacy-inprocess).
    """

    def __init__(self, base_dir: Union[str, Path], emitter=None, legacy: bool = False):
        self.base_dir = Path(base_dir)
        self.raw_dir = self.base_dir / "raw_data"
        self.cleaned_dir = self.base_dir / "cleaned"
        self.preprocessed_dir = self.base_dir / "preprocessed"
        self.watcher_dir = self.raw_dir / "_watcher"

        # emitter 가 있으면 workload 모드, legacy 면 인프로세스 실행,
        # 둘 다 아니면 watch 모드(감지만 — 워크로드는 스케줄러가 만든다)
        self.emitter = emitter
        self.legacy = legacy
        self.engine = PreprocessingEngine() if legacy else None

        # State
        self._known_files: Set[str] = set()
        self._running = False
        self._stage1_done = False
        self._pending_stage2_trigger = None
        self._pending_submit = False   # 제출 보류(선행 워크로드 진행 중)

    def _scan_images(self) -> List[str]:
        """Scan raw_data/images/ for image files."""
        img_dir = self.raw_dir / "images"
        if not img_dir.exists():
            return []

        current = set()
        for f in img_dir.iterdir():
            if f.is_file() and f.suffix.lower() in IMAGE_EXTENSIONS:
                current.add(f.name)

        new_files = current - self._known_files
        self._known_files = current
        return sorted(new_files)

    def _check_trigger(self):
        """Check for trigger.json from HOST."""
        trigger_path = self.watcher_dir / "trigger.json"
        if trigger_path.exists():
            try:
                data = json.loads(trigger_path.read_text(encoding="utf-8"))
                # Archive trigger
                archive_dir = self.watcher_dir / "archive"
                archive_dir.mkdir(parents=True, exist_ok=True)
                trigger_path.rename(archive_dir / f"trigger_{int(time.time())}.json")
                return data
            except Exception as e:
                logger.error(f"Trigger read error: {e}")
        return None

    def _stage1_output_ready(self) -> bool:
        """Return True when Stage 1 output already exists and is usable."""
        if StatusTracker.is_completed(self.cleaned_dir):
            return True

        cleaned_images = self.cleaned_dir / "images"
        if cleaned_images.exists():
            return any(f.is_file() and f.suffix.lower() in IMAGE_EXTENSIONS for f in cleaned_images.iterdir())

        return False

    def _run_stage1(self):
        """Execute Stage 1: Raw Data Refining."""
        print(f"[{timestamp()}] >> Stage 1: Raw Data Refining")

        # Clean previous results
        if self.cleaned_dir.exists():
            import shutil
            shutil.rmtree(self.cleaned_dir)

        template_path = PROJECT_ROOT / "config" / "pipeline_templates" / "stage1_raw_ingestion.yaml"
        job = Job.from_pipeline_template(
            template_path=template_path,
            input_path=str(self.raw_dir / "images"),
            output_path=str(self.cleaned_dir),
            job_id="stage1-raw-ingestion",
        )

        result = self.engine.run_job(job)

        # Display results from progress.json
        progress = StatusTracker.read_progress(self.cleaned_dir)
        if progress:
            total_images = len(self._known_files)
            for stage_name, stage_data in progress["stages"].items():
                metrics = stage_data.get("metrics", {})
                print_stage_result(stage_name, metrics, total_images)

        # Count output
        img_count = 0
        img_dir = self.cleaned_dir / "images"
        if img_dir.exists():
            img_count = len(list(img_dir.glob("*.jpg")))

        status = result.get("status", "unknown")
        elapsed = result.get("elapsed_seconds", 0)
        print(f"[{timestamp()}] << Stage 1 Complete ({elapsed}s) -> cleaned/ ({img_count} images)")
        print(f"[{timestamp()}]    Waiting for labeling or Stage 2 trigger...")
        print()
        self._stage1_done = True

    def _run_stage2(self, label_path: str = ""):
        """Execute Stage 2: Training Data Construction."""
        print(f"[{timestamp()}] >> Stage 2: Training Data Construction")

        # Clean previous results
        if self.preprocessed_dir.exists():
            import shutil
            shutil.rmtree(self.preprocessed_dir)

        # Determine label path
        if not label_path:
            # Default: look for annotations in raw_data/
            ann_dir = self.raw_dir / "annotations"
            if ann_dir.exists():
                label_path = str(ann_dir)

        if not label_path:
            raise FileNotFoundError("Stage 2 trigger received, but no annotation path was provided.")

        label_dir = Path(label_path)
        if not label_dir.exists():
            raise FileNotFoundError(f"Stage 2 annotation path does not exist: {label_dir}")

        template_path = PROJECT_ROOT / "config" / "pipeline_templates" / "stage2_training_preparation.yaml"
        job = Job.from_pipeline_template(
            template_path=template_path,
            input_path=str(self.cleaned_dir / "images"),
            output_path=str(self.preprocessed_dir),
            label_path=label_path,
            source_format="coco",
            job_id="stage2-training-prep",
        )

        result = self.engine.run_job(job)

        # Display results
        progress = StatusTracker.read_progress(self.preprocessed_dir)
        if progress:
            for stage_name, stage_data in progress["stages"].items():
                metrics = stage_data.get("metrics", {})
                total = stage_data.get("total", 0)
                print_stage_result(stage_name, metrics, total)

        # Count outputs
        train_count = len(list((self.preprocessed_dir / "train" / "images").glob("*"))) if (self.preprocessed_dir / "train" / "images").exists() else 0
        val_count = len(list((self.preprocessed_dir / "val" / "images").glob("*"))) if (self.preprocessed_dir / "val" / "images").exists() else 0
        test_count = len(list((self.preprocessed_dir / "test" / "images").glob("*"))) if (self.preprocessed_dir / "test" / "images").exists() else 0

        status = result.get("status", "unknown")
        elapsed = result.get("elapsed_seconds", 0)
        print(f"[{timestamp()}] << Stage 2 Complete ({elapsed}s) -> preprocessed/")
        print(f"           train={train_count}  val={val_count}  test={test_count}")

        # Check data.yaml
        data_yaml = self.preprocessed_dir / "data.yaml"
        if data_yaml.exists():
            print(f"           data.yaml generated (YOLO training ready)")
        print()
        print(f"[{timestamp()}]    HOST can now train:")
        print(f"           model.train(data=\"{data_yaml}\")")
        print()

    # ------------------------------------------------------------------ #
    # workload 모드 — 감지 결과를 CR 로 제출 (실행은 분산 경로)
    # ------------------------------------------------------------------ #
    def _submit_workload(self):
        """새 배치를 PreprocessingWorkload 로 제출. 선행 워크로드가 진행 중이면 보류."""
        try:
            active = self.emitter.active_workload()
            if active:
                if not self._pending_submit:
                    print(f"[{timestamp()}] .. 진행 중인 워크로드가 있어 제출 보류: {active}")
                self._pending_submit = True
                return
            name = self.emitter.submit(len(self._known_files))
        except Exception as e:
            self._pending_submit = True
            print(f"[{timestamp()}] !! 워크로드 제출 실패 ({type(e).__name__}: {e}) — 다음 폴링에 재시도")
            return

        self._pending_submit = False
        print(f"[{timestamp()}] >> PreprocessingWorkload 제출: {name} "
              f"({len(self._known_files)} images)")
        print(f"[{timestamp()}]    kubectl -n {self.emitter.namespace} get pw,pj -w")
        print(f"[{timestamp()}]    결과: {self.emitter.host_base}/pj_out/{name}/")
        print()

    def _handle_trigger_workload_mode(self, trigger: dict):
        """k8s 모드(watch/workload)의 trigger.json 처리.

        stage2 는 라벨 게이트(어노테이션 도착)가 자동으로 시작하므로 별도 트리거가
        필요 없다. 다만 HOST 가 관례 밖 경로로 라벨을 올린 경우를 위해 대기 중인
        PreprocessingJob 의 감시 경로를 바꿔주는 호환 브리지만 수행한다."""
        label_path = (trigger.get("label_path") or "").strip()
        print(f"[{timestamp()}] !! Stage 2 trigger received from HOST")
        if not label_path:
            print(f"[{timestamp()}]    라벨 게이트가 어노테이션 도착을 감시 중입니다 "
                  f"— 별도 트리거 없이 stage2 가 시작됩니다")
            return
        if self.emitter is None:
            print(f"[{timestamp()}]    (watch 모드) 라벨 경로 브리지를 하려면 "
                  f"API 접근이 필요합니다 — 경로: {label_path}")
            return
        try:
            patched = self.emitter.bridge_label_path(label_path)
        except Exception as e:
            print(f"[{timestamp()}]    라벨 경로 브리지 실패 ({type(e).__name__}: {e})")
            return
        if patched:
            print(f"[{timestamp()}]    라벨 감시 경로를 {label_path} 로 변경: {patched}")
        else:
            print(f"[{timestamp()}]    라벨 대기 중인 작업이 없어 무시 (경로: {label_path})")

    def run(self):
        """Main server loop."""
        # Ensure directories
        (self.raw_dir / "images").mkdir(parents=True, exist_ok=True)
        self.watcher_dir.mkdir(parents=True, exist_ok=True)

        print_banner(self.base_dir, self.legacy)
        if self.emitter:
            print(f"[{timestamp()}]    Mode: workload — 감지한 배치를 "
                  f"PreprocessingWorkload 로 제출합니다")
            print(f"[{timestamp()}]          입력(노드 경로): {self.emitter.input_path}")
            print(f"[{timestamp()}]          전처리 실행은 분산 경로(매니저 → pj → "
                  f"CPU/CSD 워커 → 라벨 게이트 → stage2)가 담당")
        elif self.legacy:
            print(f"[{timestamp()}]    Mode: legacy-inprocess — 이 프로세스에서 "
                  f"Stage 1/2 를 직접 실행합니다")
            print(f"[{timestamp()}]    ! 분산 경로(pw→pj→워커)와 병행하면 같은 데이터를 "
                  f"두 번 처리하고 자원 경합으로 KPI 측정이 오염됩니다")
        else:
            print(f"[{timestamp()}]    Mode: watch — 데이터 도착만 기록합니다")
            print(f"[{timestamp()}]          PreprocessingWorkload 는 스케줄러가 생성합니다 "
                  f"(자동 인입이 필요하면 --emit-workload)")
        print(f"[{timestamp()}]    Waiting for data from edge devices...")
        print()

        self._running = True
        self._stage1_done = self._stage1_output_ready() if self.legacy else False
        if self._stage1_done:
            print(f"[{timestamp()}]    Existing Stage 1 output detected in cleaned/")
            print(f"[{timestamp()}]    Stage 2 trigger can be processed immediately")
            print()

        # Initial scan (ignore existing files)
        self._scan_images()

        while self._running:
            try:
                # 0. 보류된 제출 재시도 (선행 워크로드가 끝났으면 지금 제출)
                if self._pending_submit:
                    self._submit_workload()

                # 1. Check for trigger.json (Stage 2)
                trigger = self._check_trigger()
                if trigger:
                    stage = trigger.get("stage", "")
                    if stage == "stage2" and not self.legacy:
                        self._handle_trigger_workload_mode(trigger)
                    elif stage == "stage2":
                        if not self._stage1_done:
                            self._pending_stage2_trigger = trigger
                            print(f"[{timestamp()}] !! Stage 2 trigger received before Stage 1 completion")
                            print(f"[{timestamp()}]    Trigger queued until cleaned/ is ready")
                        else:
                            label_path = trigger.get("label_path", "")
                            print(f"[{timestamp()}] !! Stage 2 trigger received from HOST")
                            self._run_stage2(label_path)
                            continue

                # 2. Check for new images (Stage 1)
                new_files = self._scan_images()
                if new_files:
                    print(f"[{timestamp()}] ** New data detected: {len(new_files)} images")

                    # Debounce: wait for more files
                    print(f"[{timestamp()}]    Waiting {DEBOUNCE_SECONDS}s for batch completion...")
                    time.sleep(DEBOUNCE_SECONDS)

                    # Scan again to catch any additional files
                    more_files = self._scan_images()
                    if more_files:
                        print(f"[{timestamp()}]    +{len(more_files)} more images arrived")

                    total = len(self._known_files)
                    print(f"[{timestamp()}]    Total: {total} images in raw_data/")
                    print()

                    if self.emitter:
                        self._submit_workload()
                        continue
                    if not self.legacy:
                        # watch 모드 — 감지만 기록. 처리는 스케줄러가 만든 워크로드로.
                        print(f"[{timestamp()}]    (watch 모드) 스케줄러가 "
                              f"PreprocessingWorkload 를 생성하면 처리됩니다")
                        print()
                        continue

                    self._run_stage1()

                    if self._pending_stage2_trigger:
                        label_path = self._pending_stage2_trigger.get("label_path", "")
                        print(f"[{timestamp()}] !! Processing queued Stage 2 trigger")
                        self._pending_stage2_trigger = None
                        self._run_stage2(label_path)
                        continue

            except KeyboardInterrupt:
                print(f"\n[{timestamp()}] Server shutting down...")
                self._running = False
                break
            except Exception as e:
                print(f"[{timestamp()}] ERROR: {e}")
                import traceback
                traceback.print_exc()

            time.sleep(POLL_INTERVAL)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="CSD Data Watcher (데이터 도착 감지)")
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=PROJECT_ROOT / "demo_data",
        help="Base directory for preprocessing data (이 프로세스가 보는 경로)",
    )
    parser.add_argument(
        "--host-base-dir",
        type=Path,
        default=None,
        help="워커 Job 이 hostPath 로 마운트할 노드 경로. 컨테이너에서 실행할 때 "
             "--base-dir 와 다르면 반드시 지정한다 (미지정 시 --base-dir 사용)",
    )
    parser.add_argument(
        "--namespace",
        default=os.environ.get("WATCH_NAMESPACE", "preprocess-csd"),
        help="PreprocessingWorkload 를 만들 네임스페이스",
    )
    parser.add_argument(
        "--emit-workload",
        action="store_true",
        help="감지한 배치를 PreprocessingWorkload 로 제출 (자동 인입). 기본은 감지만 하고 "
             "워크로드 생성은 스케줄러에 맡긴다 — 생산자를 하나로 두어 중복 제출을 막는다",
    )
    parser.add_argument(
        "--legacy-inprocess",
        action="store_true",
        help="이 프로세스에서 Stage 1/2 를 직접 실행 (k8s 없는 CSD 단독 데모). "
             "운영 경로(pw→pj→워커)와 병행하면 같은 데이터를 두 번 처리하고 자원 경합으로 "
             "KPI 측정이 오염된다 — k8s 운영 중에는 쓰지 말 것",
    )
    args = parser.parse_args()

    if args.emit_workload and args.legacy_inprocess:
        parser.error("--emit-workload 와 --legacy-inprocess 는 함께 쓸 수 없습니다")

    emitter = None
    if args.emit_workload:
        emitter = WorkloadEmitter(args.namespace, args.host_base_dir or args.base_dir)

    watcher = CSDDataWatcher(args.base_dir, emitter=emitter,
                             legacy=args.legacy_inprocess)
    watcher.run()


if __name__ == "__main__":
    main()
