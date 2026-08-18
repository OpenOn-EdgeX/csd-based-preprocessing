#!/usr/bin/env python3
"""라벨 없는 stage2 실패 경로 테스트."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from csd_preprocessor.core.status import StatusTracker
from server.test_e2e_utils import create_synthetic_coco_dataset, load_json, make_workspace, run_cli


def main() -> None:
    work = make_workspace("csd-stage2-missing-labels-")
    dataset = create_synthetic_coco_dataset(work, n_images=10, include_annotations=False)
    cleaned_dir = work / "cleaned"
    out_dir = work / "preprocessed"

    stage1 = run_cli(
        "run",
        "--template", "stage1_raw_ingestion",
        "--input", str(dataset["images"]),
        "--output", str(cleaned_dir),
    )
    if stage1.get("status") != "completed":
        raise AssertionError(f"stage1 failed during failure test: {stage1}")

    result = run_cli(
        "run",
        "--template", "stage2_training_preparation",
        "--input", str(cleaned_dir),
        "--output", str(out_dir),
    )

    if result.get("status") != "failed":
        raise AssertionError(f"stage2 should fail without labels: {result}")
    if not StatusTracker.is_failed(out_dir):
        raise AssertionError("FAILED flag missing for label-less stage2")
    if StatusTracker.is_completed(out_dir):
        raise AssertionError("COMPLETED flag should not exist for failed stage2")

    progress = StatusTracker.read_progress(out_dir)
    if progress is None:
        raise AssertionError("missing progress.json for failed stage2")
    stages = progress.get("stages", {})
    if stages.get("convert_annotation", {}).get("status") != "failed":
        raise AssertionError(f"convert_annotation should fail: {progress}")

    failed_payload = load_json(out_dir / "_status" / "FAILED")
    if "One or more stages failed" not in failed_payload.get("error", ""):
        raise AssertionError(f"unexpected FAILED payload: {failed_payload}")

    print("[PASS] stage2 missing labels failure test")
    print(f"  output: {out_dir}")
    print(f"  error: {failed_payload['error']}")


if __name__ == "__main__":
    main()
