#!/usr/bin/env python3
"""CSD worker의 non-remote 로컬 fallback 스모크 테스트."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from server.test_e2e_utils import count_files, create_synthetic_coco_dataset, load_json, make_workspace, run_worker


def main() -> None:
    work = make_workspace("csd-worker-fallback-")
    dataset = create_synthetic_coco_dataset(work, n_images=4)
    out_dir = work / "worker_out"
    dataset_dir = work / "dataset"
    files = [f"img_{idx:03d}.jpg" for idx in range(4)]

    env = dict(os.environ)
    env.update({
        "BATCH_MANIFEST_JSON": json.dumps({
            "batchId": "fallback-batch",
            "worker": "CSD",
            "files": files,
        }),
        "DATA_PATH": str(dataset["images"]),
        "OUTPUT_DIR": str(out_dir),
        "DATASET_DIR": str(dataset_dir),
        "OUTPUT_HOST_DIR": str(out_dir),
        "PREPROCESSING_STEPS": json.dumps([
            {"op": "validate", "params": {
                "check_integrity": True,
                "min_resolution": [32, 32],
                "max_file_size_mb": 100,
                "allowed_formats": ["jpg", "jpeg", "png", "bmp", "tiff", "webp"],
            }},
            {"op": "resize", "params": {
                "target_size": [640, 640],
                "method": "letterbox",
                "padding_color": [114, 114, 114],
                "interpolation": "area",
            }},
            {"op": "normalize", "params": {
                "per_channel_mean": True,
                "per_channel_std": True,
            }},
        ]),
        "BATCH_ID": "fallback-batch",
        "WORKER_TYPE": "CSD",
    })

    result, _stdout = run_worker(env)

    if result.get("batchId") != "fallback-batch":
        raise AssertionError(f"unexpected batchId: {result}")
    if result.get("inputCount") != 4 or result.get("outputCount") != 4:
        raise AssertionError(f"unexpected counts: {result}")
    if result.get("preprocessingSteps") != ["validate", "resize", "normalize"]:
        raise AssertionError(f"unexpected pipeline: {result}")
    if "offload" in result:
        raise AssertionError(f"local fallback should not report offload: {result}")

    records_path = out_dir / "records.jsonl"
    result_path = out_dir / "result.json"
    if not records_path.exists() or not result_path.exists():
        raise AssertionError("worker outputs are missing")

    records = [json.loads(line) for line in records_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if len(records) != 4:
        raise AssertionError(f"unexpected record count: {len(records)}")
    if any(record.get("status") != "ok" for record in records):
        raise AssertionError(f"unexpected record statuses: {records}")

    local_result = load_json(result_path)
    if local_result.get("outputCount") != 4:
        raise AssertionError(f"result.json mismatch: {local_result}")

    image_count = count_files(dataset_dir / "images", "*.jpg")
    if image_count != 4:
        raise AssertionError(f"expected 4 resized images, got {image_count}")

    print("[PASS] worker local fallback smoke test")
    print(f"  output: {out_dir}")
    print(f"  dataset images: {image_count}")


if __name__ == "__main__":
    main()
