"""처리량(throughput) 자동 측정 루프.

MTE/WRR 분할계획은 CPU/CSD 처리량 비율을 입력으로 받는다. 이 값을 사람이 CR 에
손으로 적으면 실측과 어긋나고, 그러면 MTE 의 전제(양측이 중앙에서 동시에 만난다)가
깨진다. 그래서 완료된 잡의 샤드 result.json 에서 실제 처리량을 측정해 프로파일에
EWMA 로 누적하고, 다음 잡의 분할계획이 그 값을 쓰도록 되먹인다.

    잡 완료 → measure_job() → ThroughputStore.update()   (ConfigMap, EWMA)
                                        ↓
    다음 잡 계획 ← ThroughputStore.get() ← 같은 (노드, CSD, 파이프라인) 키

프로파일 키를 (노드, CSD, 파이프라인 템플릿, 오프로드 모드)로 잡는 이유: 처리량은
하드웨어·스텝 구성·데이터 위치에 함께 좌우된다. 특히 오프로드 모드가 결정적이다 —
copy 는 매 실행마다 scp push/pull 이 붙어 약 2.9초 고정비가 들지만, 데이터가 공유
OCFS2 파티션에 있으면(shared-volume) CSD 가 같은 파일을 제자리에서 열어 고정비가
거의 사라진다. 두 모드를 한 키로 섞으면 EWMA 가 존재하지 않는 중간값을 학습한다.

측정 지표는 두 가지를 모두 기록한다:
    compute   = inputCount / durationMillis            — 순수 처리 성능
    effective = inputCount / (push + exec + pull)      — 전송 오버헤드 포함 실효 성능
CSD 오프로드는 데이터를 CSD 로 보내고 받아오는 시간이 실제 완료 시점을 지배한다.
MTE 의 목적이 "완료 시점 동기화"이므로 기본 지표는 effective 다 (THROUGHPUT_METRIC).

CPU 워커에는 offload 구간이 없어 effective == compute 다.
"""

import json
import os
import re
import time
from pathlib import Path
from typing import Dict, List, Optional

from kubernetes.client.rest import ApiException

# 프로파일 갱신에 쓸 지표 — effective(전송 포함, 기본) | compute(순수 처리)
DEFAULT_METRIC = os.environ.get("THROUGHPUT_METRIC", "effective").strip().lower()
# EWMA 계수: new = alpha * 측정값 + (1 - alpha) * 이전값. 1.0 이면 항상 최신값만.
EWMA_ALPHA = float(os.environ.get("THROUGHPUT_EWMA_ALPHA", "0.5"))
# 샤드가 이보다 작으면 프로세스 기동시간이 지배해 처리량이 왜곡된다 → 측정 제외.
MIN_SAMPLES = int(os.environ.get("THROUGHPUT_MIN_SAMPLES", "5"))
CONFIGMAP_NAME = os.environ.get("THROUGHPUT_CONFIGMAP", "preprocess-throughput-profile")
HISTORY_LIMIT = 10
# 공유 OCFS2 파티션의 서버측 마운트 지점 — 워커의 CSD_SHARED_LOCAL_ROOT 와 같은 값.
# 데이터셋 경로가 전부 이 아래면 CSD 가 제자리에서 실행한다(shared-volume).
SHARED_LOCAL_ROOT = os.environ.get("CSD_SHARED_LOCAL_ROOT", "/mnt/newport_1").rstrip("/")
MODE_SHARED, MODE_COPY = "shared-volume", "copy"
# 데이터셋 평균 입력 픽셀이 프로파일 학습 당시와 이 배수 이상 다르면 계수를 신뢰하지
# 않는다. resize/normalize/pHash 가 원본 픽셀을 훑으므로 장당 비용이 픽셀에 비례해
# 움직인다 — 0.28MP(COCO val2017) 로 학습한 값으로 4K(8.3MP) 잡을 계획하면 크게 어긋난다.
PIXEL_DRIFT_LIMIT = float(os.environ.get("DATASET_PIXEL_DRIFT", "1.5"))
# 계획 시점에 입력 해상도를 추정할 때 헤더만 읽어볼 파일 수(균등 간격 표본).
DATASET_SAMPLE_FILES = int(os.environ.get("DATASET_SAMPLE_FILES", "16"))

METRIC_FIELD = {"effective": "effective_tps", "compute": "compute_tps"}
TARGET_FIELD = {"CPU": "cpu_throughput", "CSD": "csd_throughput"}


def sanitize(name: str) -> str:
    """DNS-1123 label 로 정규화."""
    return re.sub(r"[^a-z0-9-]", "-", name.lower()).strip("-")[:40]


def shard_batch_id(job_id: str, target: str) -> str:
    """실행엔진이 워커 Job 에 넘기는 batchId 와 동일한 규칙."""
    return f"{sanitize(job_id)}-{target.lower()}"


def predict_offload_mode(*paths) -> str:
    """데이터셋 경로들 → CSD 오프로드 모드 예측 (계획 시점).

    입출력이 **모두** 공유 파티션 아래일 때만 워커가 제자리 실행을 시도한다
    (worker/csd_worker.py 의 _shared_remote_path 판정과 같은 규칙). 하나라도
    노드 로컬이면 전체가 copy 로 떨어진다.
    """
    given = [str(p) for p in paths if p]
    if not given:
        return MODE_COPY
    for path in given:
        p = path.rstrip("/")
        if p != SHARED_LOCAL_ROOT and not p.startswith(SHARED_LOCAL_ROOT + "/"):
            return MODE_COPY
    return MODE_SHARED


def observed_offload_mode(results: Dict[str, dict]) -> str:
    """샤드 result.json 들 → 실제로 쓰인 오프로드 모드. 알 수 없으면 빈 문자열.

    예측이 아니라 실행 결과가 정본이다 — 측정치는 실제로 일어난 모드에 기록한다.
    """
    csd = results.get("CSD") or {}
    return str((csd.get("offload") or {}).get("mode") or "")


def profile_key(placement: dict, template: str, mode: str = "") -> str:
    """(노드, CSD, 파이프라인 템플릿, 오프로드 모드) → ConfigMap data 키."""
    placement = placement or {}
    node = sanitize(str(placement.get("nodeId") or "")) or "any"
    csd = sanitize(str(placement.get("csdId") or "")) or "any"
    tpl = sanitize(str(template or "")) or "default"
    mode_part = sanitize(str(mode or "")) or MODE_COPY
    return f"{node}__{csd}__{tpl}__{mode_part}"


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _ms(value) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


# --------------------------------------------------------------------------- #
# 데이터셋 특성 (평균 입력 픽셀) — 프로파일 계수의 유효 범위를 가른다
# --------------------------------------------------------------------------- #
def sample_avg_pixels(input_dir, files: List[str],
                      limit: int = 0) -> float:
    """계획 시점에 입력 이미지의 평균 픽셀 수를 추정한다. 실패하면 0.0.

    파일을 디코딩하지 않고 헤더만 읽으므로(PIL 은 lazy open) 수천 장이어도
    표본 몇 장만 건드린다. 균등 간격 표본이라 같은 입력이면 같은 값이 나온다.
    """
    if not files:
        return 0.0
    try:
        from PIL import Image
    except ImportError:
        return 0.0

    limit = limit or DATASET_SAMPLE_FILES
    step = max(1, len(files) // limit)
    picked = files[::step][:limit]
    root = Path(input_dir)
    total, counted = 0, 0
    for name in picked:
        try:
            with Image.open(root / name) as im:
                w, h = im.size
        except Exception:
            continue                      # 깨진 파일은 표본에서 빼고 계속
        if w > 0 and h > 0:
            total += w * h
            counted += 1
    return round(total / counted, 1) if counted else 0.0


def measured_avg_pixels(results: Dict[str, dict]) -> float:
    """샤드 result.json → 실제 처리한 입력의 평균 픽셀 수. 알 수 없으면 0.0.

    resize 스텝이 원본 크기를 보고하므로 그것을 쓴다. partial.total_pixels 는
    **리사이즈 후**(640×640 고정) 값이라 입력 특성을 나타내지 못한다.
    """
    total, counted = 0.0, 0
    for result in results.values():
        metrics = ((result or {}).get("stepMetrics") or {}).get("resize") or {}
        size = metrics.get("avg_original_size") or []
        n = int((result or {}).get("inputCount") or 0)
        if len(size) == 2 and n > 0:
            try:
                total += float(size[0]) * float(size[1]) * n
                counted += n
            except (TypeError, ValueError):
                continue
    return round(total / counted, 1) if counted else 0.0


def pixel_drift(a: float, b: float) -> float:
    """두 평균 픽셀 수의 배수 차이. 한쪽이라도 모르면 0.0(판정 불가)."""
    if not a or not b or a <= 0 or b <= 0:
        return 0.0
    return round(max(a, b) / min(a, b), 4)


def dataset_mismatch(profile: Optional[dict], avg_pixels: float) -> str:
    """프로파일 계수를 이 데이터셋에 써도 되는지 — 안 되면 사유 문자열.

    판정 불가(둘 중 하나라도 픽셀 정보 없음)면 통과시킨다. 없는 정보를 이유로
    학습된 값을 버리면 매 잡이 보정 실행이 된다.
    """
    if not profile or not avg_pixels or PIXEL_DRIFT_LIMIT <= 0:
        return ""                          # 0 이하 = 가드 비활성 (운영 중 끄는 스위치)
    learned = profile.get("avg_input_pixels")
    drift = pixel_drift(learned, avg_pixels)
    if drift and drift > PIXEL_DRIFT_LIMIT:
        return (f"데이터셋 평균 입력 {avg_pixels/1e6:.2f}MP 가 프로파일 학습 당시 "
                f"{learned/1e6:.2f}MP 와 {drift}배 차이 (허용 {PIXEL_DRIFT_LIMIT}배)")
    return ""


# --------------------------------------------------------------------------- #
# 측정
# --------------------------------------------------------------------------- #
def measure_shard(result: dict) -> Optional[dict]:
    """샤드 result.json → 처리량 측정치. 측정 불가면 None.

    CSD 오프로드 결과의 durationMillis 는 **CSD 내부에서 잰 처리 시간**이고,
    전송 시간은 offload.push/pull 에 따로 있다. 실효 완료시간은 셋의 합이다.
    """
    samples = int(result.get("inputCount") or 0)
    if samples <= 0:
        return None

    compute_ms = _ms(result.get("durationMillis"))
    offload = result.get("offload") or {}
    transfer_ms = _ms(offload.get("pushMillis")) + _ms(offload.get("pullMillis"))
    exec_ms = _ms(offload.get("execMillis"))
    wall_ms = (transfer_ms + exec_ms) if offload else compute_ms
    if wall_ms <= 0:
        wall_ms = compute_ms
    if compute_ms <= 0 and wall_ms <= 0:
        return None

    measured = {
        "samples": samples,
        "compute_ms": round(compute_ms),
        "wall_ms": round(wall_ms),
        "compute_tps": round(samples / (compute_ms / 1000), 4) if compute_ms > 0 else 0.0,
        "effective_tps": round(samples / (wall_ms / 1000), 4) if wall_ms > 0 else 0.0,
    }
    if offload:
        measured["offload"] = {
            "mode": offload.get("mode", ""),
            "transfer_ms": round(transfer_ms),
            "exec_ms": round(exec_ms),
        }
    return measured


def measure_results(results: Dict[str, dict], metric: str = "") -> dict:
    """{target: result.json} → 측정 리포트.

    accepted 에는 MIN_SAMPLES 이상인 타깃만 담긴다 — 프로파일 갱신에 쓸 값.
    skipped 는 왜 제외됐는지 남긴다(작은 샤드는 기동시간이 처리량을 왜곡한다).
    """
    metric = (metric or DEFAULT_METRIC).lower()
    field = METRIC_FIELD.get(metric)
    if not field:
        raise ValueError(f"unknown throughput metric '{metric}' "
                         f"(expected {sorted(METRIC_FIELD)})")

    per_target, accepted, skipped = {}, {}, {}
    for target, result in results.items():
        measured = measure_shard(result or {})
        if not measured:
            skipped[target] = "no usable timing in result.json"
            continue
        per_target[target] = measured
        if measured["samples"] < MIN_SAMPLES:
            skipped[target] = (f"shard too small ({measured['samples']} < "
                               f"{MIN_SAMPLES} samples)")
        elif measured[field] <= 0:
            skipped[target] = f"{metric} throughput is zero"
        else:
            accepted[target] = measured[field]

    report = {"metric": metric, "per_target": per_target, "accepted": accepted}
    mode = observed_offload_mode(results)
    if mode:
        report["offload_mode"] = mode
    avg_pixels = measured_avg_pixels(results)
    if avg_pixels:
        report["avg_input_pixels"] = avg_pixels
    if skipped:
        report["skipped"] = skipped
    if len(accepted) == 2:
        report["ratio_cpu_csd"] = round(accepted["CPU"] / accepted["CSD"], 4) \
            if accepted.get("CSD") else None
    return report


def measure_job(out_root, job_id: str, targets: List[str], metric: str = "") -> dict:
    """완료된 잡의 샤드 result.json 들을 읽어 측정 리포트를 만든다."""
    shards = Path(out_root) / "_shards"
    results, missing = {}, []
    for target in targets:
        path = shards / shard_batch_id(job_id, target) / "result.json"
        if not path.exists():
            missing.append(str(path))
            continue
        try:
            results[target] = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as e:
            missing.append(f"{path} ({type(e).__name__})")

    if not results:
        return {"metric": (metric or DEFAULT_METRIC).lower(), "per_target": {},
                "accepted": {}, "missing": missing}
    report = measure_results(results, metric)
    if missing:
        report["missing"] = missing
    return report


# --------------------------------------------------------------------------- #
# 프로파일 저장소 (ConfigMap)
# --------------------------------------------------------------------------- #
class ThroughputStore:
    """측정된 처리량을 네임스페이스 ConfigMap 에 EWMA 로 누적한다.

    ConfigMap 을 쓰는 이유: 매니저가 무상태로 재시작해도 학습된 값이 남고,
    kubectl 로 그대로 확인·초기화할 수 있다(디버깅/실험 재현).
    """

    def __init__(self, core_api, namespace: str, name: str = ""):
        self.api = core_api
        self.namespace = namespace
        self.name = name or CONFIGMAP_NAME

    def _load(self) -> Dict[str, dict]:
        try:
            cm = self.api.read_namespaced_config_map(self.name, self.namespace)
        except ApiException as e:
            if e.status == 404:
                return {}
            raise
        profiles = {}
        for key, raw in (cm.data or {}).items():
            try:
                profiles[key] = json.loads(raw)
            except ValueError:
                continue
        return profiles

    def get(self, key: str) -> Optional[dict]:
        return self._load().get(key)

    def update(self, key: str, accepted: Dict[str, float], job_id: str,
               metric: str, avg_pixels: float = 0.0) -> dict:
        """측정값을 EWMA 로 반영. accepted 에 없는 타깃은 이전 값을 유지한다.

        데이터셋 특성이 학습 당시와 크게 달라졌으면 이어붙이지 않고 **리셋**한다.
        해상도가 바뀐 뒤의 측정을 이전 값과 평균 내면 어느 데이터셋에도 맞지 않는
        중간값이 남는다 — 오프로드 모드를 키로 분리한 것과 같은 이유다.
        """
        if not accepted:
            raise ValueError("no accepted measurement to record")
        current = self.get(key) or {}
        reset = dataset_mismatch(current, avg_pixels) if current else ""
        if reset:
            current = {"history": current.get("history") or []}   # 이력만 감사용 보존
        entry = {
            "metric": metric,
            "samples": int(current.get("samples", 0)) + 1,
            "updated_at": _now(),
            "ewma_alpha": EWMA_ALPHA,
        }
        for target, field in TARGET_FIELD.items():
            measured = accepted.get(target)
            previous = current.get(field)
            if measured is None:
                if previous is not None:
                    entry[field] = previous          # 이번에 못 잰 쪽은 유지
                continue
            entry[field] = round(measured if previous is None
                                 else EWMA_ALPHA * measured + (1 - EWMA_ALPHA) * previous, 4)
        if avg_pixels:
            previous_px = current.get("avg_input_pixels")
            entry["avg_input_pixels"] = round(
                avg_pixels if not previous_px
                else EWMA_ALPHA * avg_pixels + (1 - EWMA_ALPHA) * previous_px, 1)
        elif current.get("avg_input_pixels"):
            entry["avg_input_pixels"] = current["avg_input_pixels"]
        if reset:
            entry["reset_reason"] = reset
            entry["reset_at"] = entry["updated_at"]

        entry["last"] = {t.lower(): round(v, 4) for t, v in accepted.items()}
        history = list(current.get("history") or [])
        record = {"job": job_id, "at": entry["updated_at"],
                  **{t.lower(): round(v, 4) for t, v in accepted.items()}}
        if avg_pixels:
            record["mp"] = round(avg_pixels / 1e6, 3)
        if reset:
            record["reset"] = True
        history.append(record)
        entry["history"] = history[-HISTORY_LIMIT:]
        self._write(key, entry)
        return entry

    def _write(self, key: str, entry: dict):
        data = {key: json.dumps(entry, ensure_ascii=False)}
        try:
            self.api.patch_namespaced_config_map(self.name, self.namespace, {"data": data})
        except ApiException as e:
            if e.status != 404:
                raise
            self.api.create_namespaced_config_map(self.namespace, {
                "metadata": {"name": self.name, "namespace": self.namespace,
                             "labels": {"part": "preprocess-csd"}},
                "data": data,
            })
