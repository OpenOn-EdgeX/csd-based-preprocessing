#!/usr/bin/env python3
"""Preprocessing Manager.

스케줄러가 생성한 PreprocessingWorkload CR 을 watch 하여:

  Pending      → 입력 스캔 → 처리량 프로파일 조회 → 분할계획 수립(partitioners
                 플러그인, 알고리즘은 spec.workload.algorithm 또는 매니저 기본값)
                 → split_index 확정
                 → 파이프라인 해석(고정 템플릿, 기본 stage1_raw_ingestion)
                 → PreprocessingJob CR 생성(ownerRef) → Dispatched
  Dispatched~  → PreprocessingJob status 를 워크로드 status 로 역전파
                 (Running/Succeeded/Failed, progress_ratio, stage)
  Succeeded    → 샤드 result.json 에서 실제 처리량 측정 → 프로파일 갱신(EWMA)
                 → status.measured_throughput 기록 (다음 잡 계획의 입력)

전처리 단계 구성:
  stage1(샤드 병렬)  = pipelineTemplate      → 워커가 샤드별로 실행
  stage2(단일 패스)  = stage2Template        → 샤드 완료 후 실행엔진이 한 번 실행
                       (라벨 의존 + 데이터셋 전역 연산이라 샤드로 쪼갤 수 없다)
  라벨이 아직 없으면 ALLOW_PLACEHOLDER_LABELS 로 임시 라벨을 허용해 배선만
  통과시킬 수 있다 — 그때 산출 데이터셋은 학습용이 아니다.

처리량 자동 측정 루프 (MTE/WRR 분할계획의 입력):
  손으로 적은 throughput 은 실측과 어긋나기 쉽고, 그러면 MTE 의 전제(양측 완료시점
  동기화)가 성립하지 않는다. 그래서 완료된 잡의 실측치를 (노드, CSD, 파이프라인)
  프로파일에 누적해 다음 잡이 그 값을 쓴다 — controller/throughput_profile.py.
  우선순위(THROUGHPUT_SOURCE=auto 기본): 측정 프로파일 → spec.workload.throughput
  → (MTE 한정) DEFAULT_CPU_RATIO 로 보정용(calibration) 1회 실행.
  spec 값을 강제하려면 workload.throughputSource: spec.

역할 경계:
  - 분할 "계획"(알고리즘 선택·경계 산출·근거 기록)은 매니저의 일 — 여기서만 한다.
  - 전처리 스텝은 워크로드별로 "고르지 않는다". config/pipeline_templates 의 고정
    템플릿(기본 DEFAULT_PIPELINE_TEMPLATE=stage1_raw_ingestion)을 그대로 해석해
    params 까지 PJ spec.preprocessing_pipeline 으로 넘긴다 — legacy 인프로세스 경로가
    쓰는 것과 같은 YAML 이므로 두 경로의 전처리 설정이 어긋나지 않는다.
  - 실행엔진(preprocess_controller.py)은 매니저가 확정한 split_index 를 그대로
    적용해 워커를 디스패치할 뿐, 계획을 다시 세우지 않는다.
  - MTE/WRR 은 partitioners.py 에 정의가 확정되면 추가 — 매니저 코드는 불변.

워크로드 CRD 는 스케줄러 파트 스펙 확정 전 draft(k8s/preprocessing-workload-crd.yaml).
스펙 확정 시 이 파일의 필드 매핑(_plan, _sync)만 조정한다.
"""

import logging
import os
import sys
import time
from pathlib import Path

import yaml
from kubernetes import client, config
from kubernetes.client.rest import ApiException

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from controller.partitioners import (  # noqa: E402
    AUTO_BALANCED_RATIO, AUTO_SMALL_DATASET, PartitionError, get_partitioner,
    resolve_weights, select_algorithm)
from controller.throughput_profile import (  # noqa: E402
    DEFAULT_METRIC, MIN_SAMPLES, PIXEL_DRIFT_LIMIT, SHARED_LOCAL_ROOT, ThroughputStore,
    dataset_mismatch, measure_job, predict_offload_mode, profile_key, sample_avg_pixels)

GROUP, VERSION = "edgeai.keti.re.kr", "v1alpha1"
WL_PLURAL, PJ_PLURAL = "preprocessingworkloads", "preprocessingjobs"
NAMESPACE = os.environ.get("WATCH_NAMESPACE", "preprocess-csd")
DEFAULT_ALGORITHM = os.environ.get("DEFAULT_ALGORITHM", "STATIC")
DEFAULT_CPU_RATIO = float(os.environ.get("DEFAULT_CPU_RATIO", "0.5"))
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "3"))
IMG_EXTS = {".jpg", ".jpeg", ".png", ".bmp"}

# 처리량 입력 우선순위. auto = 측정 프로파일 우선(없으면 spec), spec = spec 고정.
# 워크로드별로 spec.workload.throughputSource 로 재정의할 수 있다.
DEFAULT_THROUGHPUT_SOURCE = os.environ.get("THROUGHPUT_SOURCE", "auto").strip().lower()
# 측정 프로파일이 아직 없을 때 MTE 를 실패시키는 대신 이 비율로 1회 실행해
# 실측치를 확보한다(보정 실행). 그 결과가 다음 잡부터 분할계획에 쓰인다.
CALIBRATION_CPU_RATIO = float(os.environ.get("CALIBRATION_CPU_RATIO",
                                             os.environ.get("DEFAULT_CPU_RATIO", "0.5")))
# spec 값과 실측 프로파일이 이 배수 이상 어긋나면 경고 — 손으로 적은 값이 틀렸다는 신호.
THROUGHPUT_DRIFT_WARN = float(os.environ.get("THROUGHPUT_DRIFT_WARN", "1.5"))

# 파이프라인은 워크로드별로 고르지 않고 고정 템플릿(서버 경로와 동일한 YAML)을 쓴다.
TEMPLATE_DIR = Path(__file__).resolve().parent.parent / "config" / "pipeline_templates"
DEFAULT_PIPELINE_TEMPLATE = os.environ.get("DEFAULT_PIPELINE_TEMPLATE", "stage1_raw_ingestion")
# records.jsonl 에 파일별 pHash 를 남길지 (분산실험 증빙용, 템플릿 밖의 워커 스텝)
RECORD_PHASH = os.environ.get("RECORD_PHASH", "true").lower() not in ("0", "false", "no")

# stage2(라벨링 이후 학습데이터 구성) — 샤드 완료 후 단일 패스로 실행된다.
DEFAULT_STAGE2_TEMPLATE = os.environ.get("DEFAULT_STAGE2_TEMPLATE", "stage2_training_preparation")
ENABLE_STAGE2 = os.environ.get("ENABLE_STAGE2", "true").lower() not in ("0", "false", "no")
# 실 어노테이션이 아직 없을 때 임시 라벨로 stage2 배선을 통과시킬지.
# true 로 두면 학습 불가한 데이터셋이 나오지만 전체 경로가 끝까지 돈다
# (산출물에 label_source=placeholder 로 표시된다). 실 라벨이 붙으면 false 로.
ALLOW_PLACEHOLDER_LABELS = os.environ.get("ALLOW_PLACEHOLDER_LABELS", "true").lower() \
    not in ("0", "false", "no")
# 라벨 게이트: stage1 완료 후 어노테이션이 도착할 때까지 stage2 를 붙들지 여부.
# (stage1 → HOST 라벨링 → stage2 의 "가운데" 를 표현한다. pw.spec.workload.waitForLabels
#  로 워크로드별 재정의 가능)
WAIT_FOR_LABELS = os.environ.get("WAIT_FOR_LABELS", "true").lower() not in ("0", "false", "no")
# labelPath 미지정 시 감시할 기본 어노테이션 디렉터리 이름 —
# <inputPath>/../<이름>. trigger_stage2.sh 가 어노테이션을 올리는 위치와 같은 관례.
ANNOTATION_DIRNAME = os.environ.get("ANNOTATION_DIRNAME", "annotations")
ANNOTATION_EXTS = (".json", ".xml", ".txt")

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("pj-manager")


class TemplateError(Exception):
    pass


def load_pipeline_template(name: str) -> list:
    """파이프라인 템플릿 YAML → [{op, params}] (서버 경로와 동일한 정의를 공유)."""
    path = TEMPLATE_DIR / f"{name}.yaml"
    if not path.exists():
        available = sorted(p.stem for p in TEMPLATE_DIR.glob("*.yaml"))
        raise TemplateError(f"pipeline template '{name}' not found. Available: {available}")
    template = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    stages = template.get("stages") or (template.get("pipeline") or {}).get("stages") or []
    pipeline = [{"op": s["op"], "params": s.get("params", {})}
                for s in stages if isinstance(s, dict) and s.get("op")]
    if not pipeline:
        raise TemplateError(f"pipeline template '{name}' has no stages")
    return pipeline


def resolve_pipeline(w: dict) -> tuple:
    """워크로드 → (template_name, [{op, params}]).

    기본은 고정 템플릿(DEFAULT_PIPELINE_TEMPLATE) — 매니저가 워크로드별로 스텝을
    고르지 않는다. spec.workload.stages 로 스텝을 직접 나열한 경우에만 그것을
    쓰고(파라미터는 워커 기본값), 그 외에는 spec.workload.pipelineTemplate 로
    템플릿만 바꿀 수 있다."""
    stages = w.get("stages") or {}
    if stages:
        # 명시 나열: {stage1: [validate, ...], stage2: [...]} 를 순서대로 평탄화
        ops = [op for key in sorted(stages) for op in (stages[key] or [])]
        if ops:
            return "", [{"op": op, "params": {}} for op in ops]

    name = w.get("pipelineTemplate") or DEFAULT_PIPELINE_TEMPLATE
    pipeline = load_pipeline_template(name)
    if RECORD_PHASH and not any(s["op"] == "phash" for s in pipeline):
        # 템플릿에 deduplicate 가 있으면 같은 hash_size 로 맞춘다
        dedup = next((s for s in pipeline if s["op"] == "deduplicate"), None)
        params = {"hash_size": (dedup or {}).get("params", {}).get("hash_size", 16)}
        pipeline = pipeline + [{"op": "phash", "params": params}]
    return name, pipeline


def labels_present(label_dir: str) -> bool:
    """어노테이션 디렉터리에 실제 라벨 파일이 있는지."""
    if not label_dir:
        return False
    d = Path(label_dir)
    if not d.is_dir():
        return False
    return any(p.suffix.lower() in ANNOTATION_EXTS for p in d.iterdir() if p.is_file())


def resolve_stage2(w: dict) -> tuple:
    """워크로드 → (template_name, pipeline, label_path, placeholder, wait_for_labels).

    stage2 는 라벨링 이후 단계라 샤드 병렬이 아니라 샤드 완료 후 단일 패스로 돈다.
    라벨 조달 정책:
      - dataset.labelPath 가 있으면 그 디렉터리를 쓴다. 없으면
        <inputPath>/../annotations 를 관례 경로로 삼는다(trigger_stage2.sh 와 동일).
      - waitForLabels(기본 WAIT_FOR_LABELS=true)면 어노테이션이 도착할 때까지
        실행엔진이 stage2 를 붙들어 둔다 — stage1 → 라벨링 → stage2 의 게이트.
      - 대기하지 않는데 라벨도 없으면 ALLOW_PLACEHOLDER_LABELS 일 때만 임시 라벨로
        배선을 통과시킨다(학습 불가 데이터셋 — 산출물에 placeholder 로 표시).
      - stage2Template: "" / "none" 이면 stage2 를 건너뛴다(샤드 단계까지만)."""
    if not ENABLE_STAGE2:
        return "", [], "", False, False
    name = w.get("stage2Template")
    if name is None:
        name = DEFAULT_STAGE2_TEMPLATE
    if not name or str(name).lower() in ("none", "off", "skip"):
        return "", [], "", False, False

    pipeline = load_pipeline_template(name)
    dataset = w.get("dataset") or {}
    explicit = str(dataset.get("labelPath") or "")
    label_path = explicit or str(Path(dataset["inputPath"]).parent / ANNOTATION_DIRNAME)

    wait = w.get("waitForLabels")
    wait = WAIT_FOR_LABELS if wait is None else bool(wait)

    if labels_present(label_path):
        return name, pipeline, label_path, False, False   # 이미 있으면 대기 불필요
    if wait:
        return name, pipeline, label_path, False, True    # 게이트 ON — 도착까지 대기
    if explicit:
        raise TemplateError(
            f"dataset.labelPath '{explicit}' 에 어노테이션 파일이 없습니다. "
            f"waitForLabels: true 로 대기하거나 라벨을 먼저 올리세요.")
    if not ALLOW_PLACEHOLDER_LABELS:
        raise TemplateError(
            f"stage2 template '{name}' 은 어노테이션이 필요한데 {label_path} 가 비어 있습니다. "
            f"waitForLabels: true 로 대기하거나 ALLOW_PLACEHOLDER_LABELS=true 로 "
            f"임시 라벨을 허용하세요.")
    return name, pipeline, label_path, True, False        # 임시 라벨로 진행


def resolve_plan_throughput(w: dict, profile: dict) -> tuple:
    """(cpu_tp, csd_tp, basis_dict) — 분할계획에 쓸 처리량과 그 출처.

    측정 프로파일(profile)이 양쪽 값을 모두 가지고 있으면 그것을 쓴다. 없거나
    throughputSource: spec 이면 spec.workload.throughput 을 쓴다. 둘 다 없으면
    (None, None, ...) — 호출부가 보정 실행으로 처리한다.
    """
    source = str(w.get("throughputSource") or DEFAULT_THROUGHPUT_SOURCE).lower()
    spec_tp = w.get("throughput") or {}
    spec_cpu, spec_csd = spec_tp.get("cpu"), spec_tp.get("csd")
    prof_cpu = (profile or {}).get("cpu_throughput")
    prof_csd = (profile or {}).get("csd_throughput")

    basis = {"source": source}
    if spec_cpu and spec_csd:
        basis["spec"] = {"cpu": spec_cpu, "csd": spec_csd}

    if source == "auto" and prof_cpu and prof_csd:
        basis.update({
            "cpu": prof_cpu, "csd": prof_csd, "origin": "measured",
            "metric": profile.get("metric", DEFAULT_METRIC),
            "profile_samples": profile.get("samples", 0),
            "measured_at": profile.get("updated_at", ""),
        })
        if spec_cpu and spec_csd:
            # 손으로 적은 비율과 실측 비율의 어긋남 — 보고서에 남을 근거.
            spec_ratio = float(spec_cpu) / float(spec_csd)
            measured_ratio = float(prof_cpu) / float(prof_csd)
            basis["spec_vs_measured_ratio"] = {
                "spec_cpu_per_csd": round(spec_ratio, 4),
                "measured_cpu_per_csd": round(measured_ratio, 4),
                "drift": round(max(spec_ratio, measured_ratio)
                               / min(spec_ratio, measured_ratio), 4),
            }
        return float(prof_cpu), float(prof_csd), basis

    if spec_cpu and spec_csd:
        basis.update({"cpu": spec_cpu, "csd": spec_csd, "origin": "spec"})
        if source == "auto":
            basis["note"] = "측정 프로파일 없음 — 다음 잡부터 실측치로 대체된다"
        return float(spec_cpu), float(spec_csd), basis

    basis["origin"] = "none"
    return None, None, basis


class Manager:
    def __init__(self):
        kubeconfig = os.environ.get("KUBECONFIG", "")
        if kubeconfig and Path(kubeconfig).exists():
            config.load_kube_config(kubeconfig)
        else:
            config.load_incluster_config()
        self.crd = client.CustomObjectsApi()
        self.throughput = ThroughputStore(client.CoreV1Api(), NAMESPACE)

    # ------------------------------------------------------------------ #
    def patch_status(self, name: str, status: dict):
        status["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        try:
            self.crd.patch_namespaced_custom_object_status(
                GROUP, VERSION, NAMESPACE, WL_PLURAL, name, {"status": status})
        except ApiException as e:
            log.error(f"[{name}] status patch failed: {e.reason}")

    def fail(self, name: str, msg: str):
        log.error(f"[{name}] FAILED: {msg}")
        self.patch_status(name, {"phase": "Failed", "error_message": msg[:1024]})

    # ------------------------------------------------------------------ #
    # 분할계획 수립 → PreprocessingJob 생성
    # ------------------------------------------------------------------ #
    def plan_and_dispatch(self, wl: dict):
        name = wl["metadata"]["name"]
        w = wl["spec"]["workload"]
        dataset = w["dataset"]
        input_dir = Path(dataset["inputPath"])
        out_root = dataset.get("outputPath") or f"{input_dir.parent}/pj_out/{name}"
        requested_algorithm = str(w.get("algorithm") or DEFAULT_ALGORITHM).upper()

        if not input_dir.is_dir():
            return self.fail(name, f"dataset.inputPath not found: {input_dir}")
        files = sorted(p.name for p in input_dir.iterdir() if p.suffix.lower() in IMG_EXTS)
        if not files:
            return self.fail(name, f"no images under {input_dir}")

        # --- 전처리 파이프라인: 고정 템플릿 해석 (params 포함) ---
        # 처리량 프로파일 키가 템플릿에 의존하므로(스텝 구성이 처리량을 바꾼다)
        # 분할계획보다 먼저 해석한다.
        try:
            template_name, pipeline = resolve_pipeline(w)
            s2_template, s2_pipeline, label_path, placeholder, wait_labels = resolve_stage2(w)
        except TemplateError as e:
            return self.fail(name, str(e))
        steps = [s["op"] for s in pipeline]

        # --- 분할계획 (매니저 고유 책임) ---
        placement = wl["spec"].get("placement", {})
        # 오프로드 모드가 CSD 비용 구조를 바꾸므로(copy 는 약 2.9초 고정비) 프로파일을
        # 모드별로 분리한다. 계획 시점에는 경로로 예측하고, 측정은 실제 모드에 기록한다.
        offload_mode = predict_offload_mode(input_dir, out_root, label_path)
        key = profile_key(placement, template_name, offload_mode)
        try:
            profile = self.throughput.get(key)
        except Exception as e:                      # 프로파일 조회 실패는 치명적이지 않다
            log.warning(f"[{name}] throughput profile 조회 실패({type(e).__name__}: {e}) "
                        f"— spec 값으로 진행")
            profile = None
        # 학습된 계수가 이 데이터셋에 유효한지 — 장당 비용은 원본 픽셀에 비례해 움직인다.
        # 해상도가 크게 다르면 계수를 버리고 보정 실행으로 다시 배운다.
        dataset_pixels = sample_avg_pixels(input_dir, files)
        mismatch = dataset_mismatch(profile, dataset_pixels)
        if mismatch:
            log.warning(f"[{name}] 프로파일 무시({key}): {mismatch} — 이 데이터셋으로 "
                        f"다시 측정합니다")
            profile = None
        cpu_tp, csd_tp, tp_basis = resolve_plan_throughput(w, profile)
        if dataset_pixels:
            tp_basis["dataset_avg_pixels"] = dataset_pixels
        if mismatch:
            tp_basis["profile_ignored"] = mismatch
        tp_basis["profile_key"] = key
        tp_basis["offload_mode"] = offload_mode

        # 알고리즘 선택: AUTO 면 워크로드 특성(데이터셋 규모 + 실측 성능비)으로 정한다.
        # 실측 처리량이 선택의 입력이므로 반드시 프로파일 조회 뒤에 온다.
        if requested_algorithm == "AUTO":
            algorithm, selection = select_algorithm(len(files), cpu_tp, csd_tp)
            log.info(f"[{name}] 알고리즘 자동 선택 → {algorithm}: {selection['reason']}")
        else:
            algorithm = requested_algorithm
            selection = {"mode": "explicit", "total_files": len(files),
                         "reason": f"spec.workload.algorithm={algorithm} 지정"}

        weights = w.get("weights") or {}
        plan_info = {
            "cpu_ratio": DEFAULT_CPU_RATIO,
            "split_index": 0,
            "cpu_throughput": cpu_tp,
            "csd_throughput": csd_tp,
            "cpu_weight": weights.get("cpu"),
            "csd_weight": weights.get("csd"),
        }
        # 보정(calibration) 실행: MTE 인데 쓸 처리량이 아직 없으면 실패시키지 않고
        # 기본 비율로 한 번 돌려 실측치를 확보한다 — 다음 잡부터 그 값이 쓰인다.
        calibration = False
        if algorithm == "MTE" and cpu_tp is None:
            plan_info["split_index"] = max(1, round(len(files) * CALIBRATION_CPU_RATIO))
            plan_info["cpu_ratio"] = CALIBRATION_CPU_RATIO
            calibration = True
        try:
            shards = get_partitioner(algorithm).plan(files, ["CPU", "CSD"], plan_info)
        except PartitionError as e:
            return self.fail(name, str(e))

        # split_index 는 연속 분할(STATIC/MTE)의 경계. WRR 은 비연속(가중치가 계획).
        split_index = len(shards["CPU"]) if algorithm != "WRR" else 0
        targets = [t for t in ("CPU", "CSD") if shards.get(t)]
        cpu_ratio = len(shards["CPU"]) / len(files)
        basis = {
            "source": "preprocess-manager",
            "total_files": len(files),
            "slo": w.get("slo", {}),
            "placement": placement,
            "algorithm_selection": selection,
        }
        if tp_basis.get("origin") != "none":
            # 엔진(partitioners.resolve_throughput)이 읽는 자리 — 계획에 실제로 쓴 값.
            basis["throughput"] = tp_basis
        if calibration:
            basis["calibration"] = {
                "reason": "no measured throughput profile yet",
                "cpu_ratio": CALIBRATION_CPU_RATIO,
                "profile_key": key,
            }
        if algorithm == "WRR":
            wc, ws = resolve_weights(plan_info)
            basis["weights"] = {"cpu": wc, "csd": ws}   # 엔진이 동일 가중치로 큐 재구성
        if algorithm == "STATIC" and split_index == round(len(files) * DEFAULT_CPU_RATIO):
            basis["default_cpu_ratio"] = DEFAULT_CPU_RATIO
        plan = {
            "algorithm": algorithm,
            "cpu_ratio": round(cpu_ratio, 4),
            "csd_ratio": round(1 - cpu_ratio, 4),
            "split_index": split_index,
            "basis": basis,
        }

        pj = {
            "apiVersion": f"{GROUP}/{VERSION}",
            "kind": "PreprocessingJob",
            "metadata": {
                "name": name,
                "namespace": NAMESPACE,
                "labels": {"part": "preprocess-csd", "managed-by": "preprocess-manager"},
                "ownerReferences": [{
                    "apiVersion": f"{GROUP}/{VERSION}",
                    "kind": "PreprocessingWorkload",
                    "name": name, "uid": wl["metadata"]["uid"],
                }],
            },
            "spec": {
                "job_id": name,
                "input_dataset_path": str(input_dir),
                "output_dataset_path": str(out_root),
                "preprocessing_steps": steps,          # 이름만 (관측/호환용)
                "preprocessing_pipeline": pipeline,    # params 포함 (실행 정본)
                "pipeline_template": template_name,
                "execution_targets": targets,
                "partition_info": plan,
            },
        }
        if s2_pipeline:
            pj["spec"].update({
                "stage2_template": s2_template,
                "stage2_pipeline": s2_pipeline,
                "label_dataset_path": label_path,
                "placeholder_labels": placeholder,
                "wait_for_labels": wait_labels,
            })
        try:
            self.crd.create_namespaced_custom_object(GROUP, VERSION, NAMESPACE, PJ_PLURAL, pj)
        except ApiException as e:
            if e.status != 409:  # 이미 존재 → 재시작 후 reconcile, 그대로 sync 단계로
                return self.fail(name, f"PreprocessingJob create failed: {e.reason}")

        self.patch_status(name, {
            "phase": "Dispatched",
            "progress_ratio": 0.0,
            "preprocessing_job": name,
            "partition_plan": plan,
        })
        log.info(f"[{name}] plan: {algorithm} split_index={split_index} "
                 f"(CPU {len(shards.get('CPU', []))} / CSD {len(shards.get('CSD', []))}) "
                 f"| pipeline={template_name or 'spec.stages'} {steps} "
                 f"| stage2={s2_template or '(skip)'}"
                 f"{' labels=PLACEHOLDER' if placeholder else ''}"
                 f"{' labels=WAIT:' + label_path if wait_labels else ''}"
                 f"{' labels=' + label_path if (label_path and not wait_labels and not placeholder) else ''} "
                 f"→ PreprocessingJob/{name}")
        if offload_mode != "shared-volume":
            log.warning(f"[{name}] 데이터셋이 공유 파티션({SHARED_LOCAL_ROOT}) 밖이라 CSD "
                        f"오프로드가 copy 모드로 돕니다 — 샤드마다 약 2.9초 고정비(scp "
                        f"push/pull)가 붙어 작은 데이터셋에서는 분할 이득이 사라집니다. "
                        f"inputPath/outputPath 를 {SHARED_LOCAL_ROOT} 아래로 두세요.")
        origin = tp_basis.get("origin")
        if origin == "measured":
            log.info(f"[{name}] throughput: 실측 프로파일 사용 "
                     f"(cpu={cpu_tp} csd={csd_tp} samples/s, metric={tp_basis['metric']}, "
                     f"jobs={tp_basis['profile_samples']}, key={key})")
            drift = (tp_basis.get("spec_vs_measured_ratio") or {}).get("drift")
            if drift and drift >= THROUGHPUT_DRIFT_WARN:
                d = tp_basis["spec_vs_measured_ratio"]
                log.warning(f"[{name}] spec 의 throughput 비율이 실측과 {drift}배 어긋납니다 "
                            f"(spec {d['spec_cpu_per_csd']} vs 실측 {d['measured_cpu_per_csd']} "
                            f"cpu/csd) — 실측치로 계획했습니다. spec 값을 강제하려면 "
                            f"workload.throughputSource: spec")
        elif origin == "spec":
            log.info(f"[{name}] throughput: spec 값 사용 (cpu={cpu_tp} csd={csd_tp}) "
                     f"— {tp_basis.get('note', 'throughputSource=spec')}")
        elif calibration:
            log.warning(f"[{name}] 측정 프로파일이 없어 MTE 를 보정 실행으로 돌립니다 "
                        f"(cpu_ratio={CALIBRATION_CPU_RATIO}, split_index={split_index}). "
                        f"완료 후 실측치가 {key} 에 저장되고 다음 잡부터 적용됩니다.")
        if wait_labels:
            log.info(f"[{name}] 라벨 게이트 ON — stage1 완료 후 {label_path} 에 어노테이션이 "
                     f"도착하면 stage2 가 실행됩니다")
        if placeholder:
            log.warning(f"[{name}] stage2 가 임시 라벨로 실행됩니다 — 산출 데이터셋은 학습용이 "
                        f"아닙니다. 실 어노테이션이 준비되면 dataset.labelPath 를 지정하세요.")

    # ------------------------------------------------------------------ #
    # PreprocessingJob status → 워크로드 status 역전파
    # ------------------------------------------------------------------ #
    def sync(self, wl: dict):
        name = wl["metadata"]["name"]
        try:
            pj = self.crd.get_namespaced_custom_object(
                GROUP, VERSION, NAMESPACE, PJ_PLURAL, name)
        except ApiException as e:
            if e.status == 404:
                return self.fail(name, "PreprocessingJob disappeared")
            return
        pj_status = pj.get("status") or {}
        phase_map = {"Pending": "Dispatched", "Running": "Running",
                     "Succeeded": "Succeeded", "Failed": "Failed"}
        phase = phase_map.get(pj_status.get("status", ""), "Dispatched")
        stage = pj_status.get("stage", "")
        wl_status = wl.get("status") or {}
        if (phase == wl_status.get("phase")
                and stage == wl_status.get("stage", "")
                and pj_status.get("progress_ratio") == wl_status.get("progress_ratio")):
            return  # 변화 없음
        update = {
            "phase": phase,
            "progress_ratio": pj_status.get("progress_ratio", 0.0),
            "preprocessing_job": name,
            "partition_plan": wl_status.get("partition_plan", {}),
        }
        if stage:
            update["stage"] = stage
        if phase == "Failed":
            update["error_message"] = pj_status.get("error_message", "")
        self.patch_status(name, update)
        if phase in ("Succeeded", "Failed"):
            log.info(f"[{name}] workload {phase}")

    # ------------------------------------------------------------------ #
    # 처리량 자동 측정 (Succeeded → 프로파일 갱신)
    # ------------------------------------------------------------------ #
    def finalize_measurement(self, wl: dict):
        """완료된 잡의 샤드 result.json 에서 실측 처리량을 회수해 프로파일에 반영.

        status.measured_throughput 을 반드시 채운다(측정 실패도 사유와 함께 기록) —
        비워 두면 reconcile 루프가 매 주기 재시도하게 된다.
        """
        name = wl["metadata"]["name"]
        plan = (wl.get("status") or {}).get("partition_plan") or {}
        basis = plan.get("basis") or {}
        w = wl["spec"]["workload"]

        def record(payload: dict):
            payload["measured_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            self.patch_status(name, {"phase": "Succeeded",
                                     "measured_throughput": payload})

        try:
            pj = self.crd.get_namespaced_custom_object(
                GROUP, VERSION, NAMESPACE, PJ_PLURAL, name)
        except ApiException as e:
            return record({"status": "unavailable",
                           "reason": f"PreprocessingJob 조회 실패: {e.reason}"})

        out_root = ((pj.get("status") or {}).get("output_dataset_path")
                    or pj["spec"].get("output_dataset_path"))
        targets = pj["spec"].get("execution_targets") or []
        if not out_root or not targets:
            return record({"status": "unavailable",
                           "reason": "output_dataset_path 또는 execution_targets 없음"})

        report = measure_job(out_root, pj["spec"]["job_id"], targets)
        payload = {
            "status": "measured",
            "metric": report["metric"],
            "per_target": report["per_target"],
            "min_samples": MIN_SAMPLES,
        }
        for optional in ("skipped", "missing", "ratio_cpu_csd", "offload_mode",
                         "avg_input_pixels"):
            if report.get(optional):
                payload[optional] = report[optional]

        accepted = report.get("accepted") or {}
        if not accepted:
            payload["status"] = "unavailable"
            payload.setdefault("reason", "측정 가능한 샤드 결과가 없습니다")
            log.info(f"[{name}] 처리량 측정 불가 — 프로파일 미갱신 "
                     f"({payload.get('reason')})")
            return record(payload)

        # 계획 때 예측한 모드가 실제와 다를 수 있으므로(경로 판정 실패 → copy 폴백)
        # 측정치는 **실제로 일어난 모드**의 키에 기록한다.
        planned_basis = basis.get("throughput") or {}
        mode = report.get("offload_mode") or planned_basis.get("offload_mode") or ""
        planned_key = planned_basis.get("profile_key") or basis.get("profile_key")
        key = profile_key(basis.get("placement") or wl["spec"].get("placement", {}),
                          pj["spec"].get("pipeline_template") or "", mode)
        payload["profile_key"] = key
        if planned_key and planned_key != key:
            payload["planned_profile_key"] = planned_key
            log.warning(f"[{name}] 오프로드 모드 예측({planned_basis.get('offload_mode')})과 "
                        f"실제({mode})가 달라 다른 프로파일에 기록합니다: {key}")
        payload["accepted"] = {t: round(v, 4) for t, v in accepted.items()}
        try:
            entry = self.throughput.update(key, accepted, name, report["metric"],
                                           report.get("avg_input_pixels", 0.0))
        except Exception as e:
            payload["status"] = "not_recorded"
            payload["reason"] = f"프로파일 저장 실패: {type(e).__name__}: {e}"
            log.error(f"[{name}] throughput 프로파일 저장 실패: {e}")
            return record(payload)

        payload["profile"] = {"cpu_throughput": entry.get("cpu_throughput"),
                              "csd_throughput": entry.get("csd_throughput"),
                              "avg_input_pixels": entry.get("avg_input_pixels"),
                              "samples": entry.get("samples")}
        if entry.get("reset_reason"):
            payload["profile_reset"] = entry["reset_reason"]
            log.warning(f"[{name}] 데이터셋이 바뀌어 프로파일 {key} 를 리셋하고 "
                        f"이번 측정부터 다시 학습합니다 ({entry['reset_reason']})")
        record(payload)

        measured = " ".join(f"{t}={v:.2f}" for t, v in sorted(accepted.items()))
        planned = planned_basis
        log.info(f"[{name}] 처리량 실측({report['metric']}): {measured} samples/s "
                 f"→ 프로파일 {key} 갱신 (EWMA cpu={entry.get('cpu_throughput')} "
                 f"csd={entry.get('csd_throughput')}, jobs={entry.get('samples')})")
        if planned.get("cpu") and planned.get("csd") and len(accepted) == 2 and accepted["CSD"]:
            planned_ratio = float(planned["cpu"]) / float(planned["csd"])
            actual_ratio = accepted["CPU"] / accepted["CSD"]
            drift = max(planned_ratio, actual_ratio) / min(planned_ratio, actual_ratio)
            if drift >= THROUGHPUT_DRIFT_WARN:
                log.warning(f"[{name}] 계획에 쓴 처리량 비율 {planned_ratio:.2f} vs "
                            f"실측 {actual_ratio:.2f} (cpu/csd, {drift:.2f}배 차이) — "
                            f"분할 경계가 완료시점을 맞추지 못했습니다. "
                            f"갱신된 프로파일이 다음 잡에 반영됩니다.")
        if w.get("throughputSource", "").lower() == "spec":
            log.info(f"[{name}] throughputSource=spec — 측정은 기록했지만 이 워크로드의 "
                     f"계획에는 spec 값이 쓰였습니다")

    # ------------------------------------------------------------------ #
    def reconcile_all(self):
        wls = self.crd.list_namespaced_custom_object(GROUP, VERSION, NAMESPACE, WL_PLURAL)
        for wl in wls.get("items", []):
            name = wl["metadata"]["name"]
            phase = (wl.get("status") or {}).get("phase", "")
            try:
                if phase in ("", "Pending", "Planning"):
                    self.plan_and_dispatch(wl)
                elif phase in ("Dispatched", "Running"):
                    self.sync(wl)
                elif phase == "Succeeded" and not (wl.get("status") or {}).get(
                        "measured_throughput"):
                    # 완료 직후 1회 — 이전 버전에서 끝난 잡도 여기서 소급 측정된다.
                    self.finalize_measurement(wl)
            except Exception as e:
                self.fail(name, f"{type(e).__name__}: {e}")

    def run(self):
        log.info(f"Preprocessing Manager started (ns={NAMESPACE}, "
                 f"default_algorithm={DEFAULT_ALGORITHM}, cpu_ratio={DEFAULT_CPU_RATIO}, "
                 f"throughput_source={DEFAULT_THROUGHPUT_SOURCE}, "
                 f"metric={DEFAULT_METRIC}, min_samples={MIN_SAMPLES}, "
                 f"pixel_drift_limit={PIXEL_DRIFT_LIMIT}, "
                 f"auto_policy=[small<={AUTO_SMALL_DATASET}, ratio<={AUTO_BALANCED_RATIO} → MTE])")
        while True:
            try:
                self.reconcile_all()
            except Exception as e:
                log.error(f"reconcile loop error: {type(e).__name__}: {e}")
            time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    Manager().run()
