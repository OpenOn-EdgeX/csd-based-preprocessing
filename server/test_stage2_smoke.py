#!/usr/bin/env python3
"""로컬 stage2 스모크 테스트."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from csd_preprocessor.core.status import StatusTracker
from server.test_e2e_utils import count_files, create_synthetic_coco_dataset, load_json, make_workspace, run_cli


def main() -> None:
    work = make_workspace("csd-stage2-smoke-")
    dataset = create_synthetic_coco_dataset(work, n_images=20)
    cleaned_dir = work / "cleaned"
    out_dir = work / "preprocessed"

    stage1 = run_cli(
        "run",
        "--template", "stage1_raw_ingestion",
        "--input", str(dataset["raw"]),
        "--output", str(cleaned_dir),
    )
    if stage1.get("status") != "completed":
        raise AssertionError(f"stage1 failed during stage2 smoke: {stage1}")

    result = run_cli(
        "run",
        "--template", "stage2_training_preparation",
        "--input", str(cleaned_dir),
        "--output", str(out_dir),
        "--labels", str(dataset["annotations"]),
        "--format", "coco",
    )

    if result.get("status") != "completed":
        raise AssertionError(f"unexpected result: {result}")
    if not StatusTracker.is_completed(out_dir):
        raise AssertionError("stage2 output is not marked as completed")

    data_yaml = (out_dir / "data.yaml").read_text(encoding="utf-8")
    if "train/images" not in data_yaml or "val/images" not in data_yaml or "test/images" not in data_yaml:
        raise AssertionError("data.yaml missing split paths")
    if "object" not in data_yaml:
        raise AssertionError("data.yaml missing class name")

    stats = load_json(out_dir / "statistics.json")
    if stats.get("dataset_summary", {}).get("classes") != 1:
        raise AssertionError(f"unexpected class count: {stats}")

    train_images = count_files(out_dir / "train" / "images", "*.jpg")
    val_images = count_files(out_dir / "val" / "images", "*.jpg")
    test_images = count_files(out_dir / "test" / "images", "*.jpg")
    total_images = train_images + val_images + test_images
    total_labels = (
        count_files(out_dir / "train" / "labels", "*.txt")
        + count_files(out_dir / "val" / "labels", "*.txt")
        + count_files(out_dir / "test" / "labels", "*.txt")
    )

    if train_images == 0 or val_images == 0 or test_images == 0:
        raise AssertionError(
            f"expected non-empty train/val/test splits, got train={train_images}, "
            f"val={val_images}, test={test_images}"
        )
    if total_images < 20:
        raise AssertionError(f"expected at least 20 output images, got {total_images}")
    if total_images != total_labels:
        raise AssertionError(f"image/label count mismatch: {total_images} vs {total_labels}")

    progress = StatusTracker.read_progress(out_dir)
    if progress is None:
        raise AssertionError("missing stage2 progress.json")
    if progress.get("stages", {}).get("statistics", {}).get("status") != "completed":
        raise AssertionError(f"statistics stage not completed: {progress}")

    print("[PASS] stage2 smoke test")
    print(f"  output: {out_dir}")
    print(f"  split images: train={train_images} val={val_images} test={test_images}")


if __name__ == "__main__":
    main()
