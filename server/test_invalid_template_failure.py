#!/usr/bin/env python3
"""잘못된 템플릿 이름 실패 경로 테스트."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from server.test_e2e_utils import create_synthetic_coco_dataset, make_workspace, run_cli_raw


def main() -> None:
    work = make_workspace("csd-invalid-template-")
    dataset = create_synthetic_coco_dataset(work, n_images=2)
    out_dir = work / "out"

    proc = run_cli_raw(
        "run",
        "--template", "template_that_does_not_exist",
        "--input", str(dataset["images"]),
        "--output", str(out_dir),
    )

    if proc.returncode == 0:
        raise AssertionError("cli should fail for invalid template name")
    if "Template 'template_that_does_not_exist' not found" not in proc.stdout:
        raise AssertionError(f"unexpected failure output:\n{proc.stdout}")
    if out_dir.exists() and (out_dir / "_status").exists():
        raise AssertionError("status directory should not be created for invalid template lookup")

    print("[PASS] invalid template failure test")
    print(f"  returncode: {proc.returncode}")


if __name__ == "__main__":
    main()
