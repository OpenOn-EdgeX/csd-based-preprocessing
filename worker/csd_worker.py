#!/usr/bin/env python3
"""Real CSD preprocessing worker (shard-level, file-based contract).

mock_worker.py 의 env/파일 계약(records.jsonl + result.json, cli.py mock-verify
호환)을 그대로 유지하되, mock 레코드 대신 **실제 전처리**를 배정된 샤드에 대해
수행한다. 수행할 전처리 스텝은 PreprocessingJob spec.preprocessing_steps 가
컨트롤러를 거쳐 PREPROCESSING_STEPS 로 내려온 것이며(미지정 시 기본
resize -> normalize -> pHash), 'phash' 를 뺀 이름은 csd_preprocessor 연산
레지스트리에서 해석된다 — 즉 파이프라인 템플릿(config/pipeline_templates)과 동일한
연산 구현을 샤드 단위로 재사용한다. 샤드 배정(files)은 상위 Preprocessing
Manager 의 partition_info 에서 내려온다(본 실험은 정적 STATIC 분할이며, MTE/WRR
알고리즘 선택 및 동적 스위칭은 Manager 정책으로 본 워커 범위 밖이다). 리사이즈 이미지는 공유 OUTPUT_DIR/images 에
직접 기록되고(=OCFS2 직접 기록 — 샤드 결과를 합치는 단계 자체가 없다),
부분 통계(mean/std, pixels)를 result.json 에 담아 Host 가 전역 통계로 집계할 수 있게 한다.

Env contract:
  BATCH_MANIFEST_JSON  {"batchId","worker","files":[names] | "indexes":[i..]}
  DATA_PATH            공유 입력 이미지 디렉터리 (read-only 마운트)
  OUTPUT_DIR           샤드별 출력 디렉터리 (records.jsonl + result.json)
  PREPROCESSING_STEPS  (선택) 전처리 파이프라인. 세 가지 형태를 받는다:
                         '[{"op":"resize","params":{...}}, ...]'  ← 컨트롤러가
                             pipeline_template YAML 을 해석해 내려주는 형태(params 포함)
                         '["resize","normalize","phash"]'         ← 이름만(파라미터 기본값)
                         'resize, normalize'                      ← 콤마 구분
                       미지정 시 DEFAULT_STEPS.
  LABEL_PATH           (선택) 어노테이션 디렉터리. stage2 계열(convert_annotation)이
                       파이프라인에 있으면 필요하다.
  PLACEHOLDER_LABELS   (선택) true 면 LABEL_PATH 가 비었을 때 임시 COCO 라벨을
                       자동 생성해 stage2 배선을 통과시킨다. 정답이 아니므로
                       result.json/data.yaml 에 placeholder 로 표시된다.
  DATASET_DIR          (선택) 공유 결과 데이터셋 루트. 리사이즈 이미지는
                       DATASET_DIR/images 에 기록된다. 미지정 시 OUTPUT_DIR.
                       Head/Tail 이 같은 DATASET_DIR 를 쓰면 파일 이동·결합 없이
                       하나의 데이터셋으로 co-locate 된다 (OCFS2 직접 기록).
  OUTPUT_HOST_DIR      (선택) result.outputFile 에 기록할 Host 경로
  BATCH_ID, WORKER_TYPE

Remote offload (실 CSD 실행):
  CSD_REMOTE_HOST      (선택) 예: root@10.2.1.2 — 설정되고 WORKER_TYPE=CSD 이면
                       SSH 로 이 스크립트를 CSD 내부(ARM)에서 실행한다. 방식은
                       경로를 보고 자동 선택된다:
                         shared-volume — 입출력이 전부 공유 OCFS2 파티션 아래면
                           CSD 가 같은 파일을 제자리에서 읽고 쓴다 (복사 없음)
                         copy          — 그 외에는 SCP 로 밀어넣고 결과를 회수
                       출력 계약은 동일하므로 컨트롤러 집계 로직은 영향 없음.
  CSD_REMOTE_PASS      SSH 비밀번호 (sshpass) — **필수**. 공개키 인증은 쓰지 않는다.
                       원격 실행 주체(워커 파드·다른 노드)마다 키를 뿌려야 하는 부담
                       때문에 비밀번호 인증으로 통일했다.
  CSD_REMOTE_REPO      CSD 측 코드 경로 (기본 /home/ngd/storage/csd_preprocessing)
  CSD_REMOTE_WORKDIR   CSD 측 작업 루트 (기본 /home/ngd/storage/csd_offload, copy 모드만)
  CSD_SHARED_LOCAL_ROOT / CSD_SHARED_REMOTE_ROOT
                       공유 파티션의 양쪽 마운트 지점 (기본 /mnt/newport_1 ↔
                       /home/ngd/storage). 이 아래면 인플레이스 실행.
  HOST_DATA_PATH / HOST_DATASET_DIR / HOST_LABEL_PATH
                       컨테이너 안에서는 마운트 경로만 보이므로, 공유 볼륨 판정에
                       쓸 **노드 경로**를 컨트롤러가 따로 넘긴다.

Outputs (OUTPUT_DIR):
  records.jsonl   line: {index, queuePosition, filename, phash, resized, status,
                         [droppedBy]}
                  (phash 키는 'phash' 스텝이 요청된 경우에만 존재. droppedBy 는
                   어느 스텝이 파일을 탈락시켰는지 — validate/deduplicate/
                   filter_quality/resize 구분용)
  result.json     {batchId, worker, backend, inputCount, outputCount,
                   durationMillis, throughputPerSec, outputFile,
                   partial:{mean,std,total_pixels,images_processed},
                   preprocessingSteps, preprocessingPipeline, stepMetrics,
                   [warnings]}
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

# Allow "python worker/csd_worker.py" from repo root or /app in container.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

IMG_EXTS = {".jpg", ".jpeg", ".png", ".bmp"}
TARGET_SIZE = [640, 640]

# PREPROCESSING_STEPS 미지정 시 계약 (기존 하드코딩 동작과 동일)
DEFAULT_STEPS = ("resize", "normalize", "phash")
# 레지스트리 연산 — 이름 == csd_preprocessor/operations/<name>.py 모듈명
REGISTRY_STEPS = ("validate", "deduplicate", "filter_quality", "convert_annotation",
                  "resize", "normalize", "augment", "split", "statistics", "tile")
# 레지스트리 연산이 아닌 워커 고유 스텝 — 레코드 단위 후처리로 수행
PSEUDO_STEPS = ("phash",)
# 데이터셋 전역 의미를 가지는 스텝: 샤드 단위 실행은 샤드-로컬 결과만 낸다.
# (전역 결과는 컨트롤러 집계 단계 책임 — 여기서는 경고만 남기고 실행한다)
NON_SHARDABLE_STEPS = ("deduplicate", "split", "statistics")
# 새 파일을 파생시키는 스텝 (샤드 밖 파일 유입 검사에서 제외)
DERIVING_STEPS = ("augment", "tile", "split")
# 어노테이션(라벨)이 있어야 의미가 있는 스텝 — stage2 계열
LABEL_REQUIRING_STEPS = ("convert_annotation",)
# 임시 라벨의 클래스명 — 산출물(data.yaml)에서 바로 눈에 띄게 한다
PLACEHOLDER_CLASS = "placeholder_object"
# CSD 원격 실행 제한시간(초). stage2 는 데이터셋 전체를 한 번에 처리하므로 길게 잡는다.
REMOTE_EXEC_TIMEOUT = float(os.environ.get("CSD_REMOTE_EXEC_TIMEOUT", "1800"))
# 공유 볼륨(OCFS2) 양쪽 마운트 지점. 서버는 SHARED_LOCAL_ROOT, CSD 는 SHARED_REMOTE_ROOT
# 로 **같은 파티션**을 본다. 입출력이 전부 이 아래면 scp 복사 없이 제자리에서 실행한다.
SHARED_LOCAL_ROOT = os.environ.get("CSD_SHARED_LOCAL_ROOT", "/mnt/newport_1").rstrip("/")
SHARED_REMOTE_ROOT = os.environ.get("CSD_SHARED_REMOTE_ROOT", "/home/ngd/storage").rstrip("/")
# CSD 측 코드 경로와 copy 모드 작업 루트. 2026-08-14 CSD 재구축 때 코드 배치 경로가
# csd-based-preprocessing → csd_preprocessing 으로 바뀌었다(옛 경로는 더 이상 없다).
# ※ SHARED_REMOTE_ROOT 에서 파생시키지 않는다 — 데이터 마운트 지점을 바꾸면 코드
#   경로까지 따라 움직여, 공유 볼륨 설정을 손댄 순간 원격 실행이 엉뚱한 곳을 본다.
#   두 경로를 각각 옮기려면 CSD_REMOTE_REPO / CSD_REMOTE_WORKDIR 를 쓴다.
DEFAULT_REMOTE_REPO = "/home/ngd/storage/csd_preprocessing"
DEFAULT_REMOTE_WORKDIR = "/home/ngd/storage/csd_offload"
# 스텝 이름만 내려오므로 워커가 파라미터 기본값을 정한다 (템플릿 params 는 서버 경로 전용)
STEP_PARAMS = {"resize": {"target_size": TARGET_SIZE, "method": "letterbox"}}


def _fail(msg: str, code: int = 1) -> None:
    print(msg, file=sys.stderr)
    sys.exit(code)


def _require_env(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if not v:
        _fail(f"Missing required environment variable: {name}")
    return v


def _parse_manifest(raw: str) -> Dict[str, Any]:
    try:
        m = json.loads(raw)
    except json.JSONDecodeError as exc:
        _fail(f"Invalid BATCH_MANIFEST_JSON: {exc}")
    if not isinstance(m, dict):
        _fail("BATCH_MANIFEST_JSON must decode to a JSON object")
    return m


def _resolve_files(manifest: Dict[str, Any], data_path: Path) -> List[str]:
    """Resolve the shard's file list from explicit names or indexes."""
    files = manifest.get("files")
    if isinstance(files, list) and files:
        return [str(f) for f in files]

    listing = sorted(p.name for p in data_path.iterdir() if p.suffix.lower() in IMG_EXTS)
    indexes = manifest.get("indexes")
    if isinstance(indexes, list) and indexes:
        out = []
        for i in indexes:
            if not isinstance(i, int) or not (0 <= i < len(listing)):
                _fail(f"Manifest index out of range: {i} (dataset={len(listing)})")
            out.append(listing[i])
        return out

    start, end = manifest.get("startIndex"), manifest.get("endIndex")
    if isinstance(start, int) and isinstance(end, int):
        return listing[start:end]

    if not listing:
        _fail(f"No images found under DATA_PATH={data_path}")
    return listing


def _default_pipeline() -> List[Dict[str, Any]]:
    return [{"op": op, "params": dict(STEP_PARAMS.get(op, {}))} for op in DEFAULT_STEPS]


def _parse_pipeline(raw: str) -> List[Dict[str, Any]]:
    """PREPROCESSING_STEPS 를 [{op, params}] 로 정규화.

    컨트롤러는 pipeline_template YAML 을 해석한 {op, params} 배열을 내려주고,
    수동 실행에서는 이름만 나열해도 된다(그 경우 STEP_PARAMS 기본값)."""
    raw = (raw or "").strip()
    if not raw:
        return _default_pipeline()

    if raw.startswith("["):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            _fail(f"Invalid PREPROCESSING_STEPS: {exc}")
        if not isinstance(parsed, list):
            _fail("PREPROCESSING_STEPS must decode to a JSON array")
    else:
        parsed = [s.strip() for s in raw.split(",")]

    pipeline: List[Dict[str, Any]] = []
    for item in parsed:
        if isinstance(item, str):
            op, params = item.strip(), None
        elif isinstance(item, dict):
            op = str(item.get("op", "")).strip()
            params = item.get("params")
            if params is not None and not isinstance(params, dict):
                _fail(f"params for step '{op}' must be a JSON object, got {type(params).__name__}")
        else:
            _fail(f"Invalid step entry in PREPROCESSING_STEPS: {item!r}")
        if not op:
            continue
        pipeline.append({
            "op": op,
            "params": dict(params) if params is not None else dict(STEP_PARAMS.get(op, {})),
        })

    if not pipeline:
        _fail("PREPROCESSING_STEPS is empty")

    supported = set(REGISTRY_STEPS) | set(PSEUDO_STEPS)
    unknown = sorted({s["op"] for s in pipeline} - supported)
    if unknown:
        _fail(f"Unsupported preprocessing step(s): {unknown}. "
              f"Supported: {sorted(supported)}")
    return pipeline


def _load_operation(step: str):
    """레지스트리 연산 클래스를 반환 (모듈 지연 import 로 등록 트리거).

    step 은 _parse_steps 에서 REGISTRY_STEPS 로 검증된 이름만 들어온다."""
    from importlib import import_module
    from csd_preprocessor.core.registry import get_operation

    import_module(f"csd_preprocessor.operations.{step}")
    return get_operation(step)


def _atomic_write(path: Path, content: str) -> None:
    tmp = path.with_name(f"{path.name}.tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)


def _write_placeholder_labels(data_path: Path, files: List[str], dest: Path) -> Dict[str, Any]:
    """실 어노테이션이 없을 때 쓰는 **임시** COCO 라벨 생성.

    목적은 stage2 배선(convert_annotation → augment → split → statistics)을 실제로
    통과시키는 것뿐이다. 이미지 중앙 50% 를 단일 클래스 박스로 찍으므로 정답이
    아니며, 이 라벨로 만든 데이터셋은 학습에 쓸 수 없다. 산출물(result.json,
    data.yaml, statistics.json)에 placeholder 로 표시되고 워커가 경고를 남긴다."""
    import cv2

    dest.mkdir(parents=True, exist_ok=True)
    images, annotations = [], []
    for i, name in enumerate(files, start=1):
        img = cv2.imread(str(data_path / name))
        if img is None:
            continue
        h, w = img.shape[:2]
        images.append({"id": i, "file_name": name, "width": w, "height": h})
        annotations.append({
            "id": i, "image_id": i, "category_id": 1, "iscrowd": 0,
            "bbox": [w * 0.25, h * 0.25, w * 0.5, h * 0.5],
            "area": w * h * 0.25,
        })
    coco = {
        "info": {"description": "PLACEHOLDER labels — auto-generated by csd_worker.py, "
                                "NOT ground truth. Do not train on this dataset."},
        "images": images,
        "annotations": annotations,
        "categories": [{"id": 1, "name": PLACEHOLDER_CLASS, "supercategory": "none"}],
    }
    path = dest / "instances_placeholder.json"
    _atomic_write(path, json.dumps(coco, ensure_ascii=False))
    return {"path": str(path), "images": len(images), "annotations": len(annotations)}


def _write_data_yaml(ctx: Any, out_root: Path, label_source: str) -> None:
    """YOLO 학습용 data.yaml (서버 경로 engine._generate_data_yaml 과 동일 내용).

    path 를 "." 로 두어 HOST 가 어디에 마운트해도 동작한다. PyYAML 없이 직접
    쓰므로 CSD(ARM) 측 실행에서도 의존성이 늘지 않는다."""
    names = list(ctx.class_names)
    if not names and ctx.class_mapping:
        by_id = {v: k for k, v in ctx.class_mapping.items()}
        names = [by_id[i] for i in sorted(by_id)]

    lines = ["path: .", "train: train/images", "val: val/images", "test: test/images"]
    if names:
        lines += [f"nc: {len(names)}",
                  "names: [" + ", ".join(json.dumps(n, ensure_ascii=False) for n in names) + "]"]
    if label_source == "placeholder":
        lines = ["# WARNING: label_source=placeholder — 자동 생성된 임시 라벨입니다.",
                 "#          정답이 아니므로 이 데이터셋으로 학습하지 마세요.",
                 "label_source: placeholder"] + lines
    _atomic_write(out_root / "data.yaml", "\n".join(lines) + "\n")


def _write_termination_log(result: Dict[str, Any]) -> None:
    """result 요약을 /dev/termination-log 에 기록 (kubectl describe pod 로 확인용).

    stepMetrics 는 statistics 등에서 커질 수 있고 termination-log 는 크기 제한이
    있어 제외한다 — 전체 내용은 result.json 이 정본."""
    term_log = Path("/dev/termination-log")
    if not term_log.exists():
        return
    summary = {k: v for k, v in result.items() if k != "stepMetrics"}
    try:
        _atomic_write(term_log, json.dumps(summary, ensure_ascii=False) + "\n")
    except OSError:
        pass


def _resolve_labels(pipeline: List[Dict[str, Any]], data_path: Path, out_dir: Path,
                    files: List[str], label_path: str, placeholder_ok: bool,
                    warnings: List[str]) -> tuple:
    """(label_dir, label_source, label_info) 결정.

    stage2 계열(convert_annotation)이 파이프라인에 있는데 실 라벨이 없으면,
    placeholder_ok 일 때만 임시 라벨을 만들어 배선을 통과시킨다(그 사실을
    warnings/result.json 에 남긴다). 아니면 즉시 실패 — 라벨 없이 조용히
    random split 으로 떨어지는 것보다 낫다."""
    needs_label = any(s["op"] in LABEL_REQUIRING_STEPS for s in pipeline)
    if label_path:
        d = Path(label_path)
        if not d.is_dir():
            _fail(f"LABEL_PATH does not exist: {d}")
        if not needs_label:
            return d, "provided", {}
        if not (list(d.glob("*.json")) or list(d.glob("*.xml")) or list(d.glob("*.txt"))):
            _fail(f"LABEL_PATH has no annotation files (json/xml/txt): {d}")
        return d, "provided", {}

    if not needs_label:
        return None, "none", {}
    if not placeholder_ok:
        _fail(f"pipeline needs annotations for {LABEL_REQUIRING_STEPS} but LABEL_PATH is empty. "
              f"라벨 경로를 지정하거나 PLACEHOLDER_LABELS=true 로 임시 라벨을 허용하세요.")

    info = _write_placeholder_labels(data_path, files, out_dir / "_placeholder_labels")
    warnings.append(f"label_source=placeholder — {info['annotations']} 개의 임시 라벨을 자동 생성했습니다. "
                    f"정답이 아니므로 이 데이터셋으로 학습하지 마세요.")
    return Path(info["path"]).parent, "placeholder", info


def _process_shard(data_path: Path, dataset_dir: Path, files: List[str],
                   pipeline: List[Dict[str, Any]], out_dir: Path,
                   label_path: str = "", placeholder_ok: bool = False) -> Dict[str, Any]:
    """Run the requested preprocessing pipeline over the shard, in order.

    pipeline 은 PreprocessingJob 이 지정한 [{op, params}] 이며, 레지스트리 연산은
    하나의 OperationContext 를 공유해 순차 실행된다(연산이 ctx.valid_files 를
    갱신하므로 필터/리사이즈 결과가 다음 연산에 그대로 전달된다).
    Resized images are written to dataset_dir/images (shared across shards).
    split 이 실행되면 dataset_dir 에 train/val/test + data.yaml 이 만들어진다."""
    from csd_preprocessor.operations.base import OperationContext

    warnings: List[str] = []
    label_dir, label_source, label_info = _resolve_labels(
        pipeline, data_path, out_dir, files, label_path, placeholder_ok, warnings)

    ctx = OperationContext(input_path=data_path, output_path=dataset_dir,
                           label_path=label_dir)
    ctx.valid_files = list(files)

    metrics: Dict[str, Any] = {}
    step_timings: Dict[str, int] = {}
    executed: List[str] = []
    # 어느 스텝이 파일을 탈락시켰는지 (validate/deduplicate/filter_quality/resize 구분)
    dropped_by: Dict[str, str] = {}
    shard_stems = {Path(f).stem for f in files}
    alive = set(shard_stems)

    for step in pipeline:
        name, params = step["op"], step["params"]
        if name in PSEUDO_STEPS:
            continue  # 레코드 단위 후처리 — 아래에서 수행
        if name in NON_SHARDABLE_STEPS:
            warnings.append(f"step '{name}' is dataset-global; shard-local result only")
        op = _load_operation(name)(params=params)
        errors = op.validate_params()
        if errors:
            _fail(f"Invalid params for step '{name}': {errors}")
        _step_started = time.perf_counter_ns()
        metrics[name] = op.execute(ctx)
        # 스텝별 소요시간 — 어느 연산이 병목인지 규모가 커질수록 중요해진다
        # (예: deduplicate 는 쌍 비교라 파일 수의 제곱으로 늘어난다).
        step_timings[name] = (time.perf_counter_ns() - _step_started) // 1_000_000
        executed.append(name)

        # resize 는 valid_files 를 출력 파일명으로 교체하지만 stem 은 유지된다
        survived = {Path(f).stem for f in ctx.valid_files}
        # 샤드 밖 파일을 끌어온 스텝 탐지 — 연산이 입력 디렉터리를 재스캔하면
        # 모든 워커가 전체 데이터셋을 처리해 샤드 배정이 무의미해진다.
        strays = survived - shard_stems if name not in DERIVING_STEPS else set()
        if strays:
            warnings.append(f"step '{name}' pulled in {len(strays)} file(s) outside this shard "
                            f"— shard assignment ignored by that operation")
            survived &= shard_stems
            ctx.valid_files = [f for f in ctx.valid_files if Path(f).stem in survived]
        for stem in alive - survived:
            dropped_by.setdefault(stem, name)
        alive = survived

    phash_step = next((s for s in pipeline if s["op"] == "phash"), None)
    compute_phash = None
    if phash_step is not None:
        from csd_preprocessor.operations.deduplicate import compute_phash
        hash_size = int(phash_step["params"].get("hash_size", 16))

    records = []
    for pos, name in enumerate(files):
        stem = Path(name).stem
        kept = stem in alive
        record = {"index": pos, "queuePosition": pos, "filename": name}
        if phash_step is not None:
            record["phash"] = compute_phash(data_path / name, hash_size)
        record["resized"] = "resize" in executed and kept
        if kept:
            record["status"] = "ok"
        else:
            culprit = dropped_by.get(stem, "unknown")
            record["status"] = "resize_failed" if culprit == "resize" else "dropped"
            record["droppedBy"] = culprit
        records.append(record)

    # normalize 가 스텝에 없으면 부분 통계는 0 — 컨트롤러 집계(가중 평균)에서
    # total_pixels=0 인 샤드는 자연히 기여도 0 이 된다.
    # split 이 돌았으면 데이터셋 구조가 만들어졌으므로 학습 설정도 함께 낸다
    if "split" in executed:
        _write_data_yaml(ctx, dataset_dir, label_source)

    norm = metrics.get("normalize") or {}

    return {
        "records": records,
        "label_source": label_source,
        "label_info": label_info,
        "splits": (metrics.get("split") or {}).get("splits", {}),
        "classes": list(ctx.class_names),
        "resized": (metrics.get("resize") or {}).get("resized", 0),
        "partial": {
            "mean": norm.get("mean", [0.0, 0.0, 0.0]),
            "std": norm.get("std", [0.0, 0.0, 0.0]),
            "total_pixels": norm.get("total_pixels", 0),
            "images_processed": norm.get("images_processed", 0),
        },
        "metrics": metrics,
        "step_timings": step_timings,
        "warnings": warnings,
        "executed": executed,
    }


def _remote_run(cmd: List[str], desc: str, timeout: int = 600) -> subprocess.CompletedProcess:
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if proc.returncode != 0:
        _fail(f"remote offload failed at [{desc}] (rc={proc.returncode}): "
              f"{proc.stderr.strip() or proc.stdout.strip()}")
    return proc


def _check_remote_pipeline(result: Dict[str, Any], pipeline: List[Dict[str, Any]],
                           host: str, repo: str) -> None:
    """CSD 측이 요청한 파이프라인을 그대로 실행했는지 검증.

    구버전 코드는 PREPROCESSING_STEPS 를 모르고 기본 전처리를 돌아버린다 →
    CPU 샤드와 다른 결과가 나오므로 여기서 끊는다."""
    requested = [s["op"] for s in pipeline]
    reported = result.get("preprocessingSteps")
    if reported is None:
        if pipeline != _default_pipeline():
            _fail(f"CSD 측 워커가 PREPROCESSING_STEPS 를 지원하지 않습니다 (구버전). "
                  f"요청 파이프라인 {requested} 이 무시되고 기본 전처리로 실행됐습니다. "
                  f"{host}:{repo} 의 코드를 현재 리포지터리로 갱신하세요.")
        result["preprocessingSteps"] = requested   # 기본 계약과 동일 → 결과 동등
        result["preprocessingPipeline"] = pipeline
    elif list(reported) != requested:
        _fail(f"CSD 측 실행 파이프라인이 요청과 다릅니다: "
              f"requested={requested} reported={list(reported)}")


def _shared_remote_path(local) -> str:
    """서버 경로 → CSD 가 보는 같은 파일의 경로. 공유 볼륨 밖이면 빈 문자열.

    OCFS2 파티션 하나를 서버는 /mnt/newport_1, CSD 는 /home/ngd/storage 로 마운트한다.
    같은 실체이므로 접두사만 바꾸면 CSD 가 같은 파일을 연다."""
    if not local:
        return ""
    p = str(Path(local).resolve())
    if p == SHARED_LOCAL_ROOT:
        return SHARED_REMOTE_ROOT
    if p.startswith(SHARED_LOCAL_ROOT + "/"):
        return SHARED_REMOTE_ROOT + p[len(SHARED_LOCAL_ROOT):]
    return ""


def _remote_pythonpath() -> str:
    """CSD 에서 numpy/OpenCV 를 찾을 경로. 비어 있으면 아무것도 붙이지 않는다.

    CSD 는 최소 rootfs 라 복구할 때마다 dist-packages 가 초기화된다(2026-08-11 에
    실제로 날아갔다). 그래서 패키지를 공유 OCFS2 파티션에 두고 실행 시점에
    PYTHONPATH 로 얹는다 — rootfs 가 리셋돼도 CSD 는 그대로 돈다.
    """
    p = os.environ.get("CSD_REMOTE_PYTHONPATH", "/home/ngd/storage/pylibs").strip()
    return f"PYTHONPATH={shlex.quote(p)} " if p else ""


def _ssh_commands(host: str, password: str) -> Tuple[List[str], List[str]]:
    """(ssh, scp) 커맨드 접두 — sshpass 비밀번호 인증 전용.

    공개키 인증을 명시적으로 끈다. 실행 주체(서버 셸 / 워커 컨테이너 / 다른 노드)에
    따라 키가 있기도 없기도 해서, 켜 두면 서버에서만 조용히 키로 붙고 컨테이너에서는
    실패하는 식으로 환경마다 다르게 동작한다. 어디서 돌리든 같은 경로를 타게 한다."""
    opts = ["-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=10",
            "-o", "PubkeyAuthentication=no",
            "-o", "PreferredAuthentications=password"]
    prefix = ["sshpass", "-p", password]
    return prefix + ["ssh"] + opts + [host], prefix + ["scp"] + opts


def _run_inplace(ssh, host: str, repo: str, paths: Dict[str, str], out_dir: Path,
                 batch_id: str, worker: str, pipeline: List[Dict[str, Any]],
                 files: List[str], out_host_dir: str,
                 placeholder_ok: bool) -> Dict[str, Any]:
    """공유 볼륨 인플레이스 실행 — 데이터를 복사하지 않고 CSD 가 제자리에서 처리.

    입력·출력·라벨이 모두 공유 파티션 아래이므로 CSD 는 같은 파일을 직접 읽고 쓴다.
    scp push/pull 이 사라져 데이터 크기에 비례하던 전송 비용이 0 이 된다
    (이것이 워커 docstring 의 "OCFS2 직접 기록" 이 의도한 동작이다)."""
    t0 = time.perf_counter_ns()
    # 매니페스트도 공유 볼륨(출력 디렉터리)에 두고 원격에서 읽는다 —
    # 파일 수가 많아도 커맨드라인 길이 제한에 걸리지 않는다.
    out_dir.mkdir(parents=True, exist_ok=True)
    _atomic_write(out_dir / "manifest.json",
                  json.dumps({"batchId": batch_id, "worker": worker, "files": files}))
    t1 = time.perf_counter_ns()

    env_prefix = (
        f"{_remote_pythonpath()}"
        f"BATCH_MANIFEST_JSON=\"$(cat {shlex.quote(paths['out'])}/manifest.json)\" "
        f"DATA_PATH={shlex.quote(paths['data'])} "
        f"OUTPUT_DIR={shlex.quote(paths['out'])} "
        f"DATASET_DIR={shlex.quote(paths['dataset'])} "
        f"OUTPUT_HOST_DIR={shlex.quote(out_host_dir)} "
        f"PREPROCESSING_STEPS={shlex.quote(json.dumps(pipeline))} "
        f"LABEL_PATH={shlex.quote(paths.get('labels', ''))} "
        f"PLACEHOLDER_LABELS={shlex.quote('true' if placeholder_ok else '')} "
        f"BATCH_ID={shlex.quote(batch_id)} WORKER_TYPE={shlex.quote(worker)}"
    )
    _remote_run(ssh + [f"cd {shlex.quote(repo)} && {env_prefix} "
                       f"python3 worker/csd_worker.py"], "execute on CSD (in-place)",
                timeout=REMOTE_EXEC_TIMEOUT)
    t2 = time.perf_counter_ns()

    result = json.loads((out_dir / "result.json").read_text(encoding="utf-8"))
    (out_dir / "manifest.json").unlink(missing_ok=True)
    return result, (t1 - t0), (t2 - t1), 0


def _run_remote(manifest: Dict[str, Any], data_path: Path, out_dir: Path,
                dataset_dir: Path, out_host_dir: str, batch_id: str,
                worker: str, pipeline: List[Dict[str, Any]],
                label_path: str = "", placeholder_ok: bool = False) -> Dict[str, Any]:
    """Run this shard on the real CSD.

    두 가지 방식이 있고, 경로를 보고 자동으로 고른다:
      shared-volume : 입출력이 전부 공유 OCFS2 파티션 아래 → 복사 없이 제자리 실행
      copy          : 그 외(노드 로컬 디스크 등) → scp 로 밀어넣고 결과 회수

    파일 계약(records.jsonl + result.json + dataset/images)은 로컬 실행과 동일하게
    서버 측 경로에 복원되므로 상위(컨트롤러 집계)는 실행 위치를 구분할 필요가 없다.
    파이프라인(params 포함)은 이미 해석된 상태로 넘기므로 CSD 측에 템플릿 YAML 이나
    PyYAML 이 없어도 동일한 설정으로 실행된다."""
    host = _require_env("CSD_REMOTE_HOST")
    password = _require_env("CSD_REMOTE_PASS")
    repo = os.environ.get("CSD_REMOTE_REPO", "").strip() or DEFAULT_REMOTE_REPO
    work_root = os.environ.get("CSD_REMOTE_WORKDIR", "").strip() or DEFAULT_REMOTE_WORKDIR
    rdir = f"{work_root}/{batch_id}"

    ssh, scp = _ssh_commands(host, password)

    files = _resolve_files(manifest, data_path)

    # --- 공유 볼륨이면 복사 없이 제자리 실행 ---
    # 컨테이너 안에서는 /data·/dataset 처럼 마운트 경로만 보이므로, 공유 볼륨
    # 판정에는 컨트롤러가 넘겨준 **노드 경로**(HOST_*)를 쓴다. 단독 실행 시에는
    # 컨테이너 경로 == 실제 경로이므로 그대로 사용한다.
    host_data = os.environ.get("HOST_DATA_PATH", "").strip() or str(data_path)
    host_out = out_host_dir or str(out_dir)
    host_dataset = os.environ.get("HOST_DATASET_DIR", "").strip() or str(dataset_dir)
    host_labels = os.environ.get("HOST_LABEL_PATH", "").strip() or label_path
    paths = {"data": _shared_remote_path(host_data),
             "out": _shared_remote_path(host_out),
             "dataset": _shared_remote_path(host_dataset)}
    if label_path:
        paths["labels"] = _shared_remote_path(host_labels)
    if all(paths.values()):
        # CSD 가 실제로 그 경로를 보는지 확인 후 진입 (안 보이면 복사 방식으로 폴백)
        probe = subprocess.run(ssh + [f"test -d {shlex.quote(paths['data'])}"],
                               capture_output=True, text=True, timeout=30)
        if probe.returncode == 0:
            result, push_ns, exec_ns, pull_ns = _run_inplace(
                ssh, host, repo, paths, out_dir, batch_id, worker, pipeline,
                files, out_host_dir, placeholder_ok)
            _check_remote_pipeline(result, pipeline, host, repo)
            result["offload"] = {
                "executedOn": host,
                "mode": "shared-volume",
                "sharedRoot": {"local": SHARED_LOCAL_ROOT, "remote": SHARED_REMOTE_ROOT},
                "pushMillis": push_ns // 1_000_000,
                "execMillis": exec_ns // 1_000_000,
                "pullMillis": pull_ns // 1_000_000,
            }
            _atomic_write(out_dir / "result.json",
                          json.dumps(result, indent=2, ensure_ascii=False) + "\n")
            return result
        print(f"WARNING: CSD 가 공유 경로를 보지 못해 복사 방식으로 실행합니다 "
              f"({paths['data']})", file=sys.stderr)

    t0 = time.perf_counter_ns()
    _remote_run(ssh + [f"rm -rf {shlex.quote(rdir)} && "
                       f"mkdir -p {shlex.quote(rdir)}/input {shlex.quote(rdir)}/out "
                       f"{shlex.quote(rdir)}/labels {shlex.quote(rdir)}/dataset"],
                "prepare workdir")

    _remote_run(scp + [str(data_path / f) for f in files] + [f"{host}:{rdir}/input/"],
                "push shard images")

    # 라벨 푸시 — stage2(convert_annotation)를 CSD 에서 실행하려면 어노테이션도
    # 함께 올려야 한다. HOST 가 라벨링한 결과를 CSD 로 밀어 넣는 셈.
    remote_label_dir = ""
    if label_path:
        src = Path(label_path)
        ann = sorted(p for p in src.iterdir()
                     if p.is_file() and p.suffix.lower() in (".json", ".xml", ".txt"))
        if not ann:
            _fail(f"LABEL_PATH has no annotation files to push: {src}")
        _remote_run(scp + [str(p) for p in ann] + [f"{host}:{rdir}/labels/"],
                    "push annotations")
        remote_label_dir = f"{rdir}/labels"

    # 원격에서는 파일명 목록을 명시 files 로 고정 (indexes/range 재해석 방지)
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as tmp:
        json.dump({"batchId": batch_id, "worker": worker, "files": files}, tmp)
        tmp_manifest = tmp.name
    try:
        _remote_run(scp + [tmp_manifest, f"{host}:{rdir}/manifest.json"], "push manifest")
    finally:
        os.unlink(tmp_manifest)
    t1 = time.perf_counter_ns()

    env_prefix = (
        f"{_remote_pythonpath()}"
        f"BATCH_MANIFEST_JSON=\"$(cat {shlex.quote(rdir)}/manifest.json)\" "
        f"DATA_PATH={shlex.quote(rdir)}/input "
        f"OUTPUT_DIR={shlex.quote(rdir)}/out "
        f"DATASET_DIR={shlex.quote(rdir)}/dataset "
        f"OUTPUT_HOST_DIR={shlex.quote(out_host_dir)} "
        f"PREPROCESSING_STEPS={shlex.quote(json.dumps(pipeline))} "
        f"LABEL_PATH={shlex.quote(remote_label_dir)} "
        f"PLACEHOLDER_LABELS={shlex.quote('true' if placeholder_ok else '')} "
        f"BATCH_ID={shlex.quote(batch_id)} WORKER_TYPE={shlex.quote(worker)}"
    )
    _remote_run(ssh + [f"cd {shlex.quote(repo)} && {env_prefix} "
                       f"python3 worker/csd_worker.py"], "execute on CSD",
                timeout=REMOTE_EXEC_TIMEOUT)
    t2 = time.perf_counter_ns()

    out_dir.mkdir(parents=True, exist_ok=True)
    dataset_dir.mkdir(parents=True, exist_ok=True)
    # scp 원격 소스는 호출당 1개씩 (sshpass 는 첫 연결에만 비밀번호를 넣는다).
    # 산출물 구조가 파이프라인마다 다르므로(-r + glob) 통째로 회수한다:
    #   stage1 → dataset/images/          stage2 → dataset/{train,val,test}/, data.yaml, statistics.json
    _remote_run(scp + ["-r", f"{host}:{rdir}/out/*", str(out_dir)], "pull worker outputs")
    _remote_run(scp + ["-r", f"{host}:{rdir}/dataset/*", str(dataset_dir)], "pull dataset")
    t3 = time.perf_counter_ns()

    _remote_run(ssh + [f"rm -rf {shlex.quote(rdir)}"], "cleanup workdir")

    result = json.loads((out_dir / "result.json").read_text(encoding="utf-8"))
    _check_remote_pipeline(result, pipeline, host, repo)

    result["offload"] = {
        "executedOn": host,
        "mode": "copy",
        "remoteWorkdir": rdir,
        "pushMillis": (t1 - t0) // 1_000_000,
        "execMillis": (t2 - t1) // 1_000_000,
        "pullMillis": (t3 - t2) // 1_000_000,
    }
    _atomic_write(out_dir / "result.json",
                  json.dumps(result, indent=2, ensure_ascii=False) + "\n")
    return result


def main() -> int:
    started = time.perf_counter_ns()

    manifest = _parse_manifest(_require_env("BATCH_MANIFEST_JSON"))
    data_path = Path(_require_env("DATA_PATH"))
    out_dir = Path(_require_env("OUTPUT_DIR"))
    out_host_dir = os.environ.get("OUTPUT_HOST_DIR", "").strip()
    pipeline = _parse_pipeline(os.environ.get("PREPROCESSING_STEPS", ""))
    label_path = os.environ.get("LABEL_PATH", "").strip()
    placeholder_ok = os.environ.get("PLACEHOLDER_LABELS", "").lower() in ("1", "true", "yes")

    batch_id = os.environ.get("BATCH_ID", "").strip() or str(manifest.get("batchId", "")).strip()
    if not batch_id:
        _fail("BATCH_ID or manifest.batchId must be provided")
    worker = (os.environ.get("WORKER_TYPE", "").strip()
              or str(manifest.get("worker", "")).strip() or "CSD")

    dataset_dir = Path(os.environ.get("DATASET_DIR", "").strip() or out_dir)

    if not data_path.exists():
        _fail(f"DATA_PATH does not exist: {data_path}")
    out_dir.mkdir(parents=True, exist_ok=True)
    dataset_dir.mkdir(parents=True, exist_ok=True)

    # 실 CSD 오프로드: CSD 타깃이고 원격 호스트가 지정되면 CSD 내부에서 실행.
    # (원격 측 재귀 방지: CSD 쪽 실행 환경에는 CSD_REMOTE_HOST 를 넘기지 않는다)
    if worker == "CSD" and os.environ.get("CSD_REMOTE_HOST", "").strip():
        result = _run_remote(manifest, data_path, out_dir, dataset_dir,
                             out_host_dir, batch_id, worker, pipeline,
                             label_path, placeholder_ok)
        _write_termination_log(result)
        print(json.dumps(result, ensure_ascii=False))
        return 0

    files = _resolve_files(manifest, data_path)
    shard = _process_shard(data_path, dataset_dir, files, pipeline, out_dir,
                           label_path, placeholder_ok)
    records = shard["records"]
    for w in shard["warnings"]:
        print(f"WARNING: {w}", file=sys.stderr)

    finished = time.perf_counter_ns()
    dur_ns = finished - started
    throughput = (len(records) / (dur_ns / 1e9)) if dur_ns > 0 else 0.0

    records_path = out_dir / "records.jsonl"
    reported_path = (Path(out_host_dir) / "records.jsonl") if out_host_dir else records_path

    result = {
        "batchId": batch_id,
        "worker": worker,
        "backend": "csd",
        "inputCount": len(files),
        "outputCount": len(records),
        "durationMillis": dur_ns // 1_000_000,
        "durationNanos": dur_ns,
        "throughputPerSec": round(throughput, 2),
        "outputFile": str(reported_path),
        "partial": shard["partial"],
        "resizedCount": shard["resized"],
        "preprocessingSteps": [s["op"] for s in pipeline],
        "preprocessingPipeline": pipeline,   # params 포함 — 재현성 기록
        "stepMetrics": shard["metrics"],
        "stepTimingsMillis": shard["step_timings"],
        "labelSource": shard["label_source"],   # provided | placeholder | none
    }
    if shard["label_info"]:
        result["labelInfo"] = shard["label_info"]
    if shard["splits"]:
        result["splits"] = shard["splits"]
    if shard["classes"]:
        result["classes"] = shard["classes"]
    if shard["warnings"]:
        result["warnings"] = shard["warnings"]

    jsonl = "\n".join(json.dumps(r, ensure_ascii=False) for r in records)
    _atomic_write(records_path, jsonl + "\n" if jsonl else "")
    _atomic_write(out_dir / "result.json", json.dumps(result, indent=2, ensure_ascii=False) + "\n")

    _write_termination_log(result)

    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
