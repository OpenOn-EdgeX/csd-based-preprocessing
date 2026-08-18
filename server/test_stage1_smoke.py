#!/usr/bin/env python3
"""로컬 stage1 스모크 테스트."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from csd_preprocessor.core.status import StatusTracker
from server.test_e2e_utils import count_files, create_synthetic_coco_dataset, load_json, make_workspace, run_cli


def main() -> None:
    work = make_workspace("csd-stage1-smoke-")
    dataset = create_synthetic_coco_dataset(work, n_images=6)
    out_dir = work / "cleaned"

    result = run_cli(
        "run",
        "--template", "stage1_raw_ingestion",
        "--input", str(dataset["raw"]),
        "--output", str(out_dir),
    )

    if result.get("status") != "completed":
        raise AssertionError(f"unexpected result: {result}")
    if not StatusTracker.is_completed(out_dir):
        raise AssertionError("stage1 output is not marked as completed")

    image_count = count_files(out_dir / "images", "*.jpg")
    if image_count != 6:
        raise AssertionError(f"expected 6 cleaned images, got {image_count}")

    progress = StatusTracker.read_progress(out_dir)
    if progress is None:
        raise AssertionError("missing stage1 progress.json")
    if progress.get("stages", {}).get("normalize", {}).get("status") != "completed":
        raise AssertionError(f"normalize stage not completed: {progress}")

    transform = load_json(out_dir / "resize_transform.json")
    if len(transform.get("images", {})) != 6:
        raise AssertionError("resize_transform.json does not cover all images")

    print("[PASS] stage1 smoke test")
    print(f"  output: {out_dir}")
    print(f"  cleaned images: {image_count}")


if __name__ == "__main__":
    main()
