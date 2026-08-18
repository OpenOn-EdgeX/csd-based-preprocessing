#!/usr/bin/env python3
"""PreprocessingJob CRD controller.

preprocess-csd 네임스페이스의 PreprocessingJob CR 을 폴링(reconcile)하며:

  Pending          → 입력 스캔 → partitioner(spec.partition_info.algorithm)로 샤딩
                     → 타깃별 워커 Job 생성(worker/csd_worker.py 계약) → Running
                     spec.preprocessing_steps 는 PREPROCESSING_STEPS env 로 워커에
                     전달된다(워커가 레지스트리에서 연산을 해석해 순차 실행).
  Running          → Job 완료 감시, progress_ratio 갱신
  전체 Job 완료     → 샤드 result.json 집계(전역 mean/std), 일관성 검증
                     → _shards/shard_summary.json 기록
                     (이미지는 샤드들이 같은 디렉터리에 직접 기록하므로
                      파일을 합치는 단계는 없다 — 숫자만 모은다)
  stage2 가 있으면 → 라벨 게이트: spec.label_dataset_path 에 어노테이션이 도착할
                     때까지 status.stage=waiting_labels 로 대기(stage1 → 라벨링 →
                     stage2 의 "가운데"). 도착하면 단일 패스 워커 Job 디스패치
                     (wait_for_labels=false 이고 placeholder_labels=true 면 임시 라벨로 진행)
  stage2 완료       → 데이터셋 통계 + 샤드 집계 통계 통합 → statistics.json
                     → 중간산출물 정리 → Succeeded (실패 시 Failed + error_message)

설계 원칙:
  - 상태는 전부 CR status 와 k8s Job 에서 복원 가능 → 컨트롤러 재시작 안전(무상태)
  - 분할 알고리즘은 controller/partitioners.py 플러그인 — 컨트롤러는 알고리즘 내용을 모름
  - 워커 Job 은 CR 의 ownerReference 를 가짐 → CR 삭제 시 Job 자동 GC

실행 (파드): KUBECONFIG 마운트 + /home/ngd/storage 동일 경로 마운트 필요.
"""

import json
import logging
import os
import shutil
import sys
import time
from pathlib import Path

from kubernetes import client, config
from kubernetes.client.rest import ApiException

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from controller.partitioners import get_partitioner, PartitionError  # noqa: E402
# sanitize 는 batchId 규칙을 매니저(측정 루프)와 공유해야 하므로 한 곳에만 둔다.
from controller.throughput_profile import measure_results, sanitize  # noqa: E402

GROUP, VERSION, PLURAL = "edgeai.keti.re.kr", "v1alpha1", "preprocessingjobs"
NAMESPACE = os.environ.get("WATCH_NAMESPACE", "preprocess-csd")
NODE_HOSTNAME = os.environ.get("NODE_HOSTNAME", "gpu-npu-server-02")
WORKER_IMAGE = os.environ.get("WORKER_IMAGE", "csd-preprocessor:latest")
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "3"))
# CSD 타깃 샤드를 실 CSD(10.2.1.2)로 오프로드. 빈 문자열이면 서버 내 시뮬레이션.
CSD_REMOTE_HOST = os.environ.get("CSD_REMOTE_HOST", "root@10.2.1.2")
# CSD SSH 비밀번호. 공개키 인증은 쓰지 않는다 — 워커 파드마다 키를 심어야 하는
# 부담 때문에 비밀번호 인증으로 통일했다. 비어 있으면 실 오프로드 불가(시뮬레이션만).
CSD_REMOTE_PASS = os.environ.get("CSD_REMOTE_PASS", "")
# CSD 측 코드 경로. 2026-08-14 CSD 재구축 때 csd-based-preprocessing → csd_preprocessing
# 으로 바뀌었다. 워커 기본값에 기대지 않고 여기서 명시해 넘긴다.
CSD_REMOTE_REPO = os.environ.get("CSD_REMOTE_REPO", "/home/ngd/storage/csd_preprocessing")

# csd-device-plugin 이 광고하는 확장 자원. 빈 문자열이면 자원 요청 없이 스케줄.
CSD_RESOURCE_NAME = os.environ.get("CSD_RESOURCE_NAME", "keti.re.kr/csd")
IMG_EXTS = {".jpg", ".jpeg", ".png", ".bmp"}
# 샤드 결과 집계 보관 파일 (stage2 가 statistics.json 을 덮어쓰기 때문)
# ※ "집계"는 각 샤드가 보고한 숫자를 모으는 것이다. 이미지는 샤드들이 같은
#   디렉터리에 직접 기록하므로 파일을 합치는 단계는 존재하지 않는다.
SUMMARY_FILE = "shard_summary.json"


def csd_remote_env() -> list:
    """CSD 오프로드용 env — 워커는 sshpass 비밀번호 인증으로 붙는다.

    CSD_REMOTE_REPO 를 명시적으로 넘긴다: 2026-08-14 CSD 재구축 때 코드 경로가
    바뀌어, 워커 기본값에 기대면 이미지를 다시 굽기 전까지 옛 경로를 본다."""
    return [{"name": "CSD_REMOTE_HOST", "value": CSD_REMOTE_HOST},
            {"name": "CSD_REMOTE_PASS", "value": CSD_REMOTE_PASS},
            {"name": "CSD_REMOTE_REPO", "value": CSD_REMOTE_REPO}]
# stage2 가 있을 때 샤드 단계가 차지하는 진척 비율
SHARD_PROGRESS_SPAN = 0.8
# 라벨 대기 제한(초). 0 이면 무한 대기 — 라벨링은 사람 작업이라 기본은 무한.
LABEL_WAIT_TIMEOUT = float(os.environ.get("LABEL_WAIT_TIMEOUT", "0"))
# stage2 실행 위치. CSD = 데이터셋·어노테이션을 CSD 로 밀어 넣고 CSD 내부에서 실행
# (라벨링은 HOST 가 하지만 전처리는 CSD 가 한다). CPU = 노드에서 실행.
STAGE2_TARGET = os.environ.get("STAGE2_TARGET", "CSD").upper()
ANNOTATION_EXTS = (".json", ".xml", ".txt")


def stage2_pipeline(spec: dict) -> list:
    """PJ spec 의 stage2 파이프라인 ([{op, params}]) — 없으면 빈 리스트."""
    return spec.get("stage2_pipeline") or []


# popcount — 파드 파이썬은 3.11 이지만 CSD(3.8)와 같은 폴백을 공유한다.
_BIT_COUNT = getattr(int, "bit_count", None)


def _popcount(x: int) -> int:
    return _BIT_COUNT(x) if _BIT_COUNT else bin(x).count("1")


def hamming(a: str, b: str) -> int:
    """두 pHash 비트문자열의 해밍 거리 (길이가 다르면 비교 불가로 간주).

    전역 dedup 도 쌍 비교라 파일 수의 제곱으로 늘어난다 — 정수 XOR + popcount 로
    센다(문자열 한 글자씩 비교 대비 쌍당 8.9us → 0.1us 수준)."""
    if len(a) != len(b):
        return 10 ** 6
    return _popcount(int(a, 2) ^ int(b, 2))


def global_dedup(out_root: Path, results: dict, threshold: int) -> dict:
    """샤드 경계를 넘는 중복 제거 (집계 단계).

    deduplicate 연산은 ctx.valid_files 안에서만 쌍 비교를 하므로, 샤드로 나뉜
    중복(예: 원본은 CPU 샤드, 복사본은 CSD 샤드)은 어느 샤드도 보지 못한다.
    여기서는 워커가 records.jsonl 에 이미 남긴 pHash 만 비교하면 되므로 이미지를
    다시 읽지 않는다 — 무거운 해시 계산은 샤드(CSD 포함)에서 이미 끝났다.

    중복으로 판정된 파일의 리사이즈 결과는 삭제하지 않고 _duplicates/ 로 옮겨
    증빙을 남긴다. stage2 입력은 out_root/images 이므로 자동으로 제외된다."""
    records = []
    for target, r in results.items():
        rp = out_root / "_shards" / r["batchId"] / "records.jsonl"
        if not rp.exists():
            continue
        for line in rp.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            rec = json.loads(line)
            if rec.get("status") == "ok" and rec.get("phash"):
                records.append((rec["filename"], rec["phash"], target))

    records.sort(key=lambda x: x[0])           # 결정론적: 파일명 순으로 앞의 것을 남긴다
    # 유지 목록과의 비교가 O(n^2) 이므로 정수 변환을 미리 해둔다.
    hash_len = len(records[0][1]) if records else 0
    kept, groups, dropped = [], [], []
    for name, ph, target in records:
        ph_int = int(ph, 2) if len(ph) == hash_len else None
        dup_of = next((k for k, kph, kint in kept
                       if (_popcount(kint ^ ph_int) if (kint is not None and ph_int is not None)
                           else hamming(kph, ph)) <= threshold), None)
        if dup_of is None:
            kept.append((name, ph, ph_int))
            continue
        dropped.append(name)
        hit = next((g for g in groups if g["keep"] == dup_of), None)
        if hit is None:
            groups.append({"keep": dup_of, "dropped": [name]})
        else:
            hit["dropped"].append(name)

    moved = 0
    if dropped:
        dup_dir = out_root / "_duplicates"
        dup_dir.mkdir(parents=True, exist_ok=True)
        for name in dropped:
            src = out_root / "images" / f"{Path(name).stem}.jpg"
            if src.exists():
                shutil.move(str(src), str(dup_dir / src.name))
                moved += 1

    return {
        "threshold": threshold,
        "compared": len(records),
        "unique": len(kept),
        "duplicates": len(dropped),
        "images_moved": moved,
        "groups": groups[:50],          # 기록은 상위 50 그룹까지만
        "moved_to": str(out_root / "_duplicates") if dropped else "",
    }


def dedup_threshold(spec: dict):
    """stage1 파이프라인에 deduplicate 가 있으면 그 threshold, 없으면 None."""
    for step in spec.get("preprocessing_pipeline") or []:
        if step.get("op") == "deduplicate":
            return int((step.get("params") or {}).get("threshold", 5))
    return None


def labels_present(label_dir: str) -> bool:
    """어노테이션 디렉터리에 실제 라벨 파일이 도착했는지 (라벨 게이트 판정)."""
    if not label_dir:
        return False
    d = Path(label_dir)
    if not d.is_dir():
        return False
    return any(p.suffix.lower() in ANNOTATION_EXTS for p in d.iterdir() if p.is_file())


def utcnow() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def elapsed_since(stamp: str) -> float:
    try:
        return max(0.0, time.time() - time.mktime(time.strptime(stamp, "%Y-%m-%dT%H:%M:%SZ"))
                   + time.timezone)
    except (ValueError, TypeError):
        return 0.0

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("pj-controller")


def job_name(job_id: str, target: str) -> str:
    return f"pj-{sanitize(job_id)}-{target.lower()}"


class Controller:
    def __init__(self):
        kubeconfig = os.environ.get("KUBECONFIG", "")
        if kubeconfig and Path(kubeconfig).exists():
            config.load_kube_config(kubeconfig)
        else:
            config.load_incluster_config()
        self.crd = client.CustomObjectsApi()
        self.batch = client.BatchV1Api()

    # ------------------------------------------------------------------ #
    # CR status helpers
    # ------------------------------------------------------------------ #
    def patch_status(self, name: str, status: dict):
        status["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        try:
            self.crd.patch_namespaced_custom_object_status(
                GROUP, VERSION, NAMESPACE, PLURAL, name, {"status": status})
        except ApiException as e:
            log.error(f"[{name}] status patch failed: {e.reason}")

    def fail(self, name: str, msg: str):
        log.error(f"[{name}] FAILED: {msg}")
        self.patch_status(name, {"status": "Failed", "error_message": msg[:1024]})

    # ------------------------------------------------------------------ #
    # Dispatch (Pending → Running)
    # ------------------------------------------------------------------ #
    def dispatch(self, cr: dict):
        name = cr["metadata"]["name"]
        spec = cr["spec"]
        job_id = spec["job_id"]
        input_dir = Path(spec["input_dataset_path"])
        out_root = Path(spec.get("output_dataset_path") or f"{input_dir.parent}/pj_out/{job_id}")
        targets = spec["execution_targets"]
        info = spec.get("partition_info", {})

        if not input_dir.is_dir():
            return self.fail(name, f"input_dataset_path not found: {input_dir}")
        files = sorted(p.name for p in input_dir.iterdir() if p.suffix.lower() in IMG_EXTS)
        if not files:
            return self.fail(name, f"no images under {input_dir}")

        try:
            shards = get_partitioner(info.get("algorithm", "STATIC")).plan(files, targets, info)
        except PartitionError as e:
            return self.fail(name, str(e))

        out_root.mkdir(parents=True, exist_ok=True)
        for target, shard in shards.items():
            self.create_worker_job(cr, target, shard, input_dir, out_root)

        self.patch_status(name, {
            "status": "Running",
            "progress_ratio": 0.0,
            "output_dataset_path": str(out_root),
            "observed_counts": {
                "total": len(files),
                "shards": {t: len(s) for t, s in shards.items()},
            },
        })
        log.info(f"[{name}] dispatched {len(shards)} worker job(s): "
                 + ", ".join(f"{t}={len(s)}" for t, s in shards.items())
                 + f" | pipeline={spec.get('pipeline_template') or 'spec.stages'}"
                 + f" {spec.get('preprocessing_steps') or '(worker default)'}")

    def create_worker_job(self, cr: dict, target: str, shard: list,
                          input_dir: Path, out_root: Path):
        job_id = cr["spec"]["job_id"]
        batch_id = f"{sanitize(job_id)}-{target.lower()}"
        shard_dir = f"{out_root}/_shards"
        manifest = json.dumps({"batchId": batch_id, "worker": target, "files": shard})
        # 워커가 실행할 전처리 파이프라인. params 를 담은 preprocessing_pipeline
        # (매니저가 템플릿에서 해석)을 우선 쓰고, 없으면 이름만인 preprocessing_steps.
        # 둘 다 비면 env 를 넣지 않고 워커 기본 계약(DEFAULT_STEPS)에 맡긴다.
        pipeline = cr["spec"].get("preprocessing_pipeline") or [
            str(s) for s in (cr["spec"].get("preprocessing_steps") or [])
        ]

        env = [
            {"name": "BATCH_MANIFEST_JSON", "value": manifest},
            {"name": "DATA_PATH", "value": "/data"},
            {"name": "OUTPUT_DIR", "value": f"/output/{batch_id}"},
            {"name": "DATASET_DIR", "value": "/dataset"},
            {"name": "OUTPUT_HOST_DIR", "value": f"{shard_dir}/{batch_id}"},
            # 컨테이너 경로(/data, /dataset)로는 공유 볼륨 여부를 알 수 없다.
            # 워커가 CSD 인플레이스 실행을 판단하려면 **노드 경로**가 필요하다.
            {"name": "HOST_DATA_PATH", "value": str(input_dir)},
            {"name": "HOST_DATASET_DIR", "value": str(out_root)},
            {"name": "WORKER_TYPE", "value": target},
            {"name": "BATCH_ID", "value": batch_id},
        ]
        if pipeline:
            env.append({"name": "PREPROCESSING_STEPS", "value": json.dumps(pipeline)})
        if target == "CSD" and CSD_REMOTE_HOST:
            env += csd_remote_env()

        mounts = [
            {"name": "input", "mountPath": "/data", "readOnly": True},
            {"name": "output", "mountPath": "/output"},
            {"name": "dataset", "mountPath": "/dataset"},
        ]
        volumes = [
            {"name": "input",
             "hostPath": {"path": str(input_dir), "type": "Directory"}},
            {"name": "output",
             "hostPath": {"path": shard_dir, "type": "DirectoryOrCreate"}},
            {"name": "dataset",
             "hostPath": {"path": str(out_root), "type": "DirectoryOrCreate"}},
        ]
        resources = {
            "requests": {"cpu": "2", "memory": "4Gi"},
            "limits": {"cpu": "2", "memory": "4Gi"},
        }
        if target == "CSD" and CSD_RESOURCE_NAME:
            # 실 CSD 오프로드 시 확장 자원 점유 — Unhealthy(장치 불통)면 스케줄 차단,
            # 동시 점유는 DEVICE_COUNT(현재 1)로 직렬화된다.
            resources["requests"][CSD_RESOURCE_NAME] = "1"
            resources["limits"][CSD_RESOURCE_NAME] = "1"
        body = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": job_name(job_id, target),
                "namespace": NAMESPACE,
                "labels": {"app": "pj-worker", "part": "preprocess-csd",
                           "pj": sanitize(cr["metadata"]["name"])},
                "ownerReferences": [{
                    "apiVersion": f"{GROUP}/{VERSION}", "kind": "PreprocessingJob",
                    "name": cr["metadata"]["name"], "uid": cr["metadata"]["uid"],
                }],
            },
            "spec": {
                "backoffLimit": 1,
                "template": {
                    "metadata": {"labels": {"app": "pj-worker", "part": "preprocess-csd"}},
                    "spec": {
                        "serviceAccountName": "preprocess-admin",
                        "restartPolicy": "Never",
                        "nodeSelector": {"kubernetes.io/hostname": NODE_HOSTNAME},
                        "containers": [{
                            "name": "worker",
                            "image": WORKER_IMAGE,
                            "imagePullPolicy": "Never",
                            "command": ["python", "worker/csd_worker.py"],
                            "env": env,
                            "resources": resources,
                            "volumeMounts": mounts,
                        }],
                        "volumes": volumes,
                    },
                },
            },
        }
        try:
            self.batch.create_namespaced_job(NAMESPACE, body)
        except ApiException as e:
            if e.status != 409:  # already exists → 재시작 후 reconcile, 무시
                raise

    # ------------------------------------------------------------------ #
    # Progress (Running → Succeeded/Failed)
    # ------------------------------------------------------------------ #
    def job_state(self, jname: str) -> str:
        """Job → running | succeeded | failed | missing."""
        try:
            j = self.batch.read_namespaced_job(jname, NAMESPACE)
        except ApiException:
            return "missing"
        if (j.status.succeeded or 0) >= 1:
            return "succeeded"
        if (j.status.failed or 0) > (j.spec.backoff_limit or 0):
            return "failed"
        return "running"

    def check_running(self, cr: dict):
        """status.stage 에 따라 샤드 / 라벨대기 / stage2 단계를 각각 감시."""
        stage = (cr.get("status") or {}).get("stage")
        if stage == "stage2":
            return self.check_stage2(cr)
        if stage == "waiting_labels":
            return self.check_waiting_labels(cr)

        name = cr["metadata"]["name"]
        spec = cr["spec"]
        targets = spec["execution_targets"]
        status = cr.get("status", {})

        done, failed = [], []
        for target in targets:
            state = self.job_state(job_name(spec["job_id"], target))
            if state == "succeeded":
                done.append(target)
            elif state == "failed":
                failed.append(target)

        if failed:
            return self.fail(name, f"worker job failed for target(s): {failed} "
                                   f"(kubectl -n {NAMESPACE} describe job "
                                   f"{job_name(spec['job_id'], failed[0])})")
        # stage2 가 있으면 샤드 단계는 전체 진척의 80% 까지만 차지한다
        span = SHARD_PROGRESS_SPAN if stage2_pipeline(spec) else 1.0
        progress = round(len(done) / max(len(targets), 1) * span, 4)
        if len(done) < len(targets):
            if abs(progress - float(status.get("progress_ratio") or 0)) > 1e-9:
                self.patch_status(name, {"status": "Running", "stage": "shards",
                                         "progress_ratio": progress})
            return

        summary = self.aggregate_shards(cr)
        if summary is None:
            return  # fail() 이 이미 호출됨
        if stage2_pipeline(spec):
            self.enter_stage2(cr, summary)
        else:
            self.finalize(cr, summary)

    # ------------------------------------------------------------------ #
    # 라벨 게이트 (stage1 완료 → 라벨링 대기 → stage2)
    # ------------------------------------------------------------------ #
    def enter_stage2(self, cr: dict, summary: dict):
        """샤드 완료 후 stage2 진입 판단 — 라벨 유무/대기 정책에 따라 분기."""
        name = cr["metadata"]["name"]
        spec = cr["spec"]
        label_path = spec.get("label_dataset_path", "")

        if labels_present(label_path):
            return self.dispatch_stage2(cr, label_path)
        if spec.get("wait_for_labels"):
            self.patch_status(name, {
                "status": "Running",
                "stage": "waiting_labels",
                "progress_ratio": SHARD_PROGRESS_SPAN,
                "stage2": {"template": spec.get("stage2_template", ""),
                           "steps": [s["op"] for s in stage2_pipeline(spec)],
                           "label_source": "waiting",
                           "label_path": label_path,
                           "waiting_since": utcnow()},
            })
            log.info(f"[{name}] stage1 완료 — 라벨 대기 중 (watch: {label_path}). "
                     f"어노테이션을 올리면 stage2 가 자동으로 실행됩니다")
            return
        if spec.get("placeholder_labels"):
            return self.dispatch_stage2(cr, "")
        self.fail(name, f"stage2 needs annotations but none found at {label_path} "
                        f"(wait_for_labels=false, placeholder_labels=false)")

    def check_waiting_labels(self, cr: dict):
        """라벨 도착 감시 — 도착하면 stage2 디스패치, 제한시간 초과면 Failed."""
        name = cr["metadata"]["name"]
        spec = cr["spec"]
        status = cr.get("status") or {}
        label_path = spec.get("label_dataset_path", "")

        if labels_present(label_path):
            log.info(f"[{name}] 어노테이션 도착 — stage2 시작 ({label_path})")
            return self.dispatch_stage2(cr, label_path)

        since = (status.get("stage2") or {}).get("waiting_since", "")
        if LABEL_WAIT_TIMEOUT > 0 and since and elapsed_since(since) > LABEL_WAIT_TIMEOUT:
            self.fail(name, f"label wait timed out after {LABEL_WAIT_TIMEOUT:.0f}s "
                            f"(watching {label_path}). 라벨을 올리거나 "
                            f"placeholder_labels/waitForLabels 설정을 조정하세요.")

    # ------------------------------------------------------------------ #
    # Stage 2 (샤드 완료 후 단일 패스 — 라벨 필요 + 데이터셋 전역 연산)
    # ------------------------------------------------------------------ #
    def dispatch_stage2(self, cr: dict, label_path: str):
        """stage2 워커 Job 디스패치. label_path 가 빈 문자열이면 임시 라벨 모드."""
        name = cr["metadata"]["name"]
        spec = cr["spec"]
        out_root = Path(cr["status"]["output_dataset_path"])
        pipeline = stage2_pipeline(spec)
        placeholder = not label_path and bool(spec.get("placeholder_labels", False))

        if not label_path and not placeholder:
            return self.fail(name, "stage2 pipeline requires annotations but "
                                   "no labels are available and "
                                   "spec.placeholder_labels is false")
        self.create_stage2_job(cr, pipeline, out_root, label_path, placeholder)
        if placeholder:
            log.warning(f"[{name}] stage2 with PLACEHOLDER labels — 산출 데이터셋은 "
                        f"학습용이 아닙니다 (data.yaml.label_source=placeholder)")
        self.patch_status(name, {
            "status": "Running",
            "stage": "stage2",
            "progress_ratio": SHARD_PROGRESS_SPAN,
            "stage2": {"template": spec.get("stage2_template", ""),
                       "steps": [s["op"] for s in pipeline],
                       "label_source": "provided" if label_path else "placeholder",
                       "label_path": label_path,
                       "target": STAGE2_TARGET},
        })
        log.info(f"[{name}] stage2 dispatched on {STAGE2_TARGET}: "
                 f"{[s['op'] for s in pipeline]} "
                 f"(labels={label_path or 'PLACEHOLDER'})")

    def create_stage2_job(self, cr: dict, pipeline: list, out_root: Path,
                          label_path: str, placeholder: bool):
        """샤드들이 함께 기록한 데이터셋 전체를 입력으로 하는 단일 워커 Job.

        입력은 out_root/images (stage1 산출물), 출력은 out_root 자체 →
        train/val/test + data.yaml + statistics.json 이 여기에 만들어진다.

        샤드 병렬이 아닌 단일 패스인 이유는 라벨 의존 + 데이터셋 전역 연산이기
        때문이지, CPU 에서 돌아야 해서가 아니다. STAGE2_TARGET=CSD(기본)이면
        워커가 데이터셋과 어노테이션을 CSD 로 밀어 넣고 CSD 내부에서 실행한다
        — 라벨링은 HOST 가 하지만 전처리는 CSD 에서 수행한다."""
        job_id = cr["spec"]["job_id"]
        batch_id = f"{sanitize(job_id)}-stage2"
        shard_dir = f"{out_root}/_shards"
        target = STAGE2_TARGET

        env = [
            {"name": "BATCH_MANIFEST_JSON", "value": json.dumps({"batchId": batch_id,
                                                                 "worker": target})},
            {"name": "DATA_PATH", "value": "/dataset/images"},
            {"name": "OUTPUT_DIR", "value": f"/output/{batch_id}"},
            {"name": "DATASET_DIR", "value": "/dataset"},
            {"name": "OUTPUT_HOST_DIR", "value": f"{shard_dir}/{batch_id}"},
            {"name": "HOST_DATA_PATH", "value": f"{out_root}/images"},
            {"name": "HOST_DATASET_DIR", "value": str(out_root)},
            {"name": "WORKER_TYPE", "value": target},
            {"name": "BATCH_ID", "value": batch_id},
            {"name": "PREPROCESSING_STEPS", "value": json.dumps(pipeline)},
        ]
        if target == "CSD" and CSD_REMOTE_HOST:
            env += csd_remote_env()
        volumes = [
            {"name": "output", "hostPath": {"path": shard_dir, "type": "DirectoryOrCreate"}},
            {"name": "dataset", "hostPath": {"path": str(out_root), "type": "Directory"}},
        ]
        mounts = [
            {"name": "output", "mountPath": "/output"},
            {"name": "dataset", "mountPath": "/dataset"},
        ]
        if label_path:
            env.append({"name": "LABEL_PATH", "value": "/labels"})
            env.append({"name": "HOST_LABEL_PATH", "value": label_path})
            volumes.append({"name": "labels",
                            "hostPath": {"path": label_path, "type": "Directory"}})
            mounts.append({"name": "labels", "mountPath": "/labels", "readOnly": True})
        elif placeholder:
            env.append({"name": "PLACEHOLDER_LABELS", "value": "true"})

        resources = {"requests": {"cpu": "2", "memory": "4Gi"},
                     "limits": {"cpu": "2", "memory": "4Gi"}}
        if target == "CSD" and CSD_RESOURCE_NAME:
            # 샤드 단계와 같은 확장 자원을 점유 — 장치 불통이면 스케줄 차단되고,
            # DEVICE_COUNT(현재 1)로 CSD 사용이 직렬화된다.
            resources["requests"][CSD_RESOURCE_NAME] = "1"
            resources["limits"][CSD_RESOURCE_NAME] = "1"

        body = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": job_name(job_id, "stage2"),
                "namespace": NAMESPACE,
                "labels": {"app": "pj-worker", "part": "preprocess-csd", "stage": "stage2",
                           "pj": sanitize(cr["metadata"]["name"])},
                "ownerReferences": [{
                    "apiVersion": f"{GROUP}/{VERSION}", "kind": "PreprocessingJob",
                    "name": cr["metadata"]["name"], "uid": cr["metadata"]["uid"],
                }],
            },
            "spec": {
                "backoffLimit": 1,
                "template": {
                    "metadata": {"labels": {"app": "pj-worker", "part": "preprocess-csd",
                                            "stage": "stage2"}},
                    "spec": {
                        "serviceAccountName": "preprocess-admin",
                        "restartPolicy": "Never",
                        "nodeSelector": {"kubernetes.io/hostname": NODE_HOSTNAME},
                        "containers": [{
                            "name": "worker",
                            "image": WORKER_IMAGE,
                            "imagePullPolicy": "Never",
                            "command": ["python", "worker/csd_worker.py"],
                            "env": env,
                            "resources": resources,
                            "volumeMounts": mounts,
                        }],
                        "volumes": volumes,
                    },
                },
            },
        }
        try:
            self.batch.create_namespaced_job(NAMESPACE, body)
        except ApiException as e:
            if e.status != 409:
                raise

    def check_stage2(self, cr: dict):
        name = cr["metadata"]["name"]
        jname = job_name(cr["spec"]["job_id"], "stage2")
        state = self.job_state(jname)
        if state == "failed":
            return self.fail(name, f"stage2 job failed "
                                   f"(kubectl -n {NAMESPACE} describe job {jname})")
        if state == "succeeded":
            return self.finalize_stage2(cr)
        # missing = 컨트롤러 재시작 등으로 Job 이 사라진 경우 → 재생성
        if state == "missing" and self.read_summary(cr) is not None:
            label_path = cr["spec"].get("label_dataset_path", "")
            log.info(f"[{name}] stage2 job missing → re-dispatch")
            self.dispatch_stage2(cr, label_path if labels_present(label_path) else "")

    def finalize_stage2(self, cr: dict):
        """stage2 완료 → 통계 통합(샤드 집계 + 데이터셋 통계) + 정리 + Succeeded."""
        name = cr["metadata"]["name"]
        spec = cr["spec"]
        out_root = Path(cr["status"]["output_dataset_path"])
        batch_id = f"{sanitize(spec['job_id'])}-stage2"

        rp = out_root / "_shards" / batch_id / "result.json"
        if not rp.exists():
            return self.fail(name, f"missing stage2 result: {rp}")
        s2 = json.loads(rp.read_text(encoding="utf-8"))
        summary = self.read_summary(cr)
        if summary is None:
            return self.fail(name, f"missing shard summary record: "
                                   f"{out_root}/_shards/{SUMMARY_FILE}")

        # stage2 의 statistics 연산이 statistics.json 을 덮어썼으므로 샤드 집계
        # 결과를 다시 합쳐 하나의 정본으로 만든다.
        stats_path = out_root / "statistics.json"
        stats = {}
        if stats_path.exists():
            try:
                stats = json.loads(stats_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                stats = {}
        stats["shard_summary"] = summary
        stats["stage2"] = {
            "template": spec.get("stage2_template", ""),
            "steps": s2.get("preprocessingSteps", []),
            "label_source": s2.get("labelSource", ""),
            "splits": s2.get("splits", {}),
            "classes": s2.get("classes", []),
            "executed_on": (s2.get("offload") or {}).get("executedOn", "node"),
        }
        if s2.get("labelSource") == "placeholder":
            stats["stage2"]["warning"] = ("PLACEHOLDER labels — 자동 생성된 임시 라벨입니다. "
                                          "이 데이터셋으로 학습하지 마세요.")
        stats_path.write_text(json.dumps(stats, indent=2, ensure_ascii=False), encoding="utf-8")

        self.cleanup_intermediates(out_root)
        status = {
            "status": "Succeeded",
            "stage": "stage2",
            "progress_ratio": 1.0,
            "output_dataset_path": str(out_root),
            "observed_counts": cr["status"].get("observed_counts", {}),
            "consistency": summary.get("consistency", {}),
            "stage2": stats["stage2"],
        }
        self.patch_status(name, status)
        log.info(f"[{name}] Succeeded — stage2 splits={s2.get('splits', {})} "
                 f"labels={s2.get('labelSource')} → {out_root}/data.yaml")

    def cleanup_intermediates(self, out_root: Path):
        """중간 작업물 제거 — 서버 경로 engine._cleanup_intermediates 와 같은 정책."""
        work_dir = out_root / "_work"
        if work_dir.exists():
            shutil.rmtree(work_dir, ignore_errors=True)
        # split 이 끝나 train/ 이 있으면 최상위 images/ 는 중간 산출물
        if (out_root / "train").exists() and (out_root / "images").exists():
            shutil.rmtree(out_root / "images", ignore_errors=True)
            log.info(f"cleaned up intermediate directory: {out_root}/images")

    def read_summary(self, cr: dict):
        p = Path(cr["status"]["output_dataset_path"]) / "_shards" / SUMMARY_FILE
        if not p.exists():
            return None
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return None

    def aggregate_shards(self, cr: dict):
        """모든 샤드 완료 → 부분 통계 집계 + 일관성 검증. 성공 시 집계 결과 반환.

        각 샤드가 result.json 에 보고한 숫자(mean/std/pixels/카운트)만 읽는다.
        이미지 파일은 샤드들이 이미 같은 DATASET_DIR 에 직접 기록했으므로
        여기서 파일을 옮기거나 합치지 않는다."""
        name = cr["metadata"]["name"]
        spec = cr["spec"]
        out_root = Path(cr["status"]["output_dataset_path"])
        expected = cr["status"]["observed_counts"]

        results = {}
        for target in spec["execution_targets"]:
            batch_id = f"{sanitize(spec['job_id'])}-{target.lower()}"
            rp = out_root / "_shards" / batch_id / "result.json"
            if not rp.exists():
                return self.fail(name, f"missing shard result: {rp}")
            results[target] = json.loads(rp.read_text(encoding="utf-8"))

        # 전역 mean/std: 픽셀 수 가중 평균
        total_px = sum(r["partial"]["total_pixels"] for r in results.values())
        mean = [0.0] * 3
        var = [0.0] * 3
        for r in results.values():
            w = r["partial"]["total_pixels"] / total_px if total_px else 0
            for c in range(3):
                mean[c] += w * r["partial"]["mean"][c]
        for r in results.values():
            w = r["partial"]["total_pixels"] / total_px if total_px else 0
            for c in range(3):
                d = r["partial"]["mean"][c] - mean[c]
                var[c] += w * (r["partial"]["std"][c] ** 2 + d * d)
        std = [v ** 0.5 for v in var]

        reported_steps = next((r["preprocessingSteps"] for r in results.values()
                               if r.get("preprocessingSteps")), [])
        reported_pipeline = next((r["preprocessingPipeline"] for r in results.values()
                                 if r.get("preprocessingPipeline")), [])
        input_sum = sum(r["inputCount"] for r in results.values())
        output_sum = sum(r["outputCount"] for r in results.values())
        consistency = {
            "input_total_matches": input_sum == expected["total"],
            "input_total": input_sum,
            "output_total": output_sum,
            "per_target": {t: {"input": r["inputCount"], "output": r["outputCount"],
                               "duration_ms": r["durationMillis"]}
                           for t, r in results.items()},
        }
        stats = {
            "job_id": spec["job_id"],
            "algorithm": spec.get("partition_info", {}).get("algorithm", "STATIC"),
            "pipeline_template": spec.get("pipeline_template", ""),
            # 실제 워커가 수행한 스텝 (샤드 result.json 이 정본, 미보고 시 spec 값)
            "preprocessing_steps": reported_steps or spec.get("preprocessing_steps", []),
            "preprocessing_pipeline": reported_pipeline or spec.get("preprocessing_pipeline", []),
            "global_mean": [round(x, 6) for x in mean],
            "global_std": [round(x, 6) for x in std],
            "total_pixels": total_px,
            "consistency": consistency,
            # 실측 처리량 — 매니저가 이 값을 프로파일에 누적해 다음 잡의 분할계획에 쓴다.
            # (매니저는 샤드 result.json 을 직접 읽지만, 감사용으로 여기에도 남긴다)
            "measured_throughput": measure_results(results),
        }
        if not consistency["input_total_matches"]:
            self.fail(name, f"consistency check failed: input {input_sum} "
                            f"!= expected {expected['total']}")
            return None

        # 샤드 경계를 넘는 중복 제거 (stage1 에 deduplicate 가 있을 때만)
        threshold = dedup_threshold(spec)
        if threshold is not None:
            gd = global_dedup(out_root, results, threshold)
            stats["global_dedup"] = gd
            if gd["duplicates"]:
                log.info(f"[{name}] global dedup: {gd['duplicates']} cross-shard duplicate(s) "
                         f"moved to _duplicates/ ({gd['unique']}/{gd['compared']} unique)")
            # 전역 통계는 중복 픽셀을 포함한 값이다(샤드가 집계값만 보고하므로 차감 불가).
            # 정확한 값이 필요하면 stage2 템플릿에 normalize 를 추가해 재계산한다.
            stats["global_mean_includes_duplicates"] = bool(gd["duplicates"])

        # stage2 가 statistics.json 을 덮어쓰므로 샤드 집계 결과는 별도 파일에
        # 보관한다(컨트롤러 재시작 후에도 복원 가능 — 무상태 유지).
        stats["observed_counts"] = {**expected, "output_total": output_sum}
        (out_root / "_shards" / SUMMARY_FILE).write_text(
            json.dumps(stats, indent=2, ensure_ascii=False), encoding="utf-8")
        log.info(f"[{name}] shard results aggregated — {output_sum}/{expected['total']} files")
        return stats

    def finalize(self, cr: dict, summary: dict):
        """stage2 없는 경우: 샤드 집계 결과를 정본 통계로 쓰고 종료."""
        name = cr["metadata"]["name"]
        out_root = Path(cr["status"]["output_dataset_path"])
        (out_root / "statistics.json").write_text(
            json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
        self.cleanup_intermediates(out_root)
        self.patch_status(name, {
            "status": "Succeeded",
            "stage": "shards",
            "progress_ratio": 1.0,
            "output_dataset_path": str(out_root),
            "observed_counts": summary.get("observed_counts", {}),
            "consistency": summary.get("consistency", {}),
        })
        log.info(f"[{name}] Succeeded — stats → {out_root}/statistics.json")

    # ------------------------------------------------------------------ #
    def reconcile_all(self):
        crs = self.crd.list_namespaced_custom_object(GROUP, VERSION, NAMESPACE, PLURAL)
        for cr in crs.get("items", []):
            name = cr["metadata"]["name"]
            phase = (cr.get("status") or {}).get("status", "")
            try:
                if phase in ("", "Pending"):
                    self.dispatch(cr)
                elif phase == "Running":
                    self.check_running(cr)
            except Exception as e:  # 개별 CR 실패가 루프를 죽이지 않게
                self.fail(name, f"{type(e).__name__}: {e}")

    def run(self):
        log.info(f"PreprocessingJob controller started (ns={NAMESPACE}, "
                 f"node={NODE_HOSTNAME}, image={WORKER_IMAGE})")
        while True:
            try:
                self.reconcile_all()
            except Exception as e:
                log.error(f"reconcile loop error: {type(e).__name__}: {e}")
            time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    Controller().run()
