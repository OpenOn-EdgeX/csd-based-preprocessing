#!/usr/bin/env python3
"""Local test for host-side mock worker verification and merge flow."""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from csd_preprocessor.io.mock_batch import merge_mock_batches, verify_mock_batch


def make_batch(root: Path, batch_id: str, indexes: list[int]) -> Path:
    batch_dir = root / batch_id
    batch_dir.mkdir(parents=True, exist_ok=True)

    records = [
        {
            "index": index,
            "queuePosition": position,
            "value": f"mock-result-{index}",
            "worker": "CSD",
        }
        for position, index in enumerate(indexes)
    ]
    (batch_dir / "records.jsonl").write_text(
        "".join(json.dumps(record, ensure_ascii=False) + "\n" for record in records),
        encoding="utf-8",
    )

    result = {
        "batchId": batch_id,
        "worker": "CSD",
        "backend": "csd",
        "inputCount": len(indexes),
        "outputCount": len(indexes),
        "durationMillis": 0,
        "durationNanos": 1234,
        "throughputPerSec": 1000.0,
        "outputFile": str(batch_dir / "records.jsonl"),
    }
    (batch_dir / "result.json").write_text(
        json.dumps(result, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    (batch_dir / "stdout.json").write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")
    return batch_dir


def run_single_batch_verification() -> None:
    root = Path(tempfile.mkdtemp(prefix="csd-mock-verify-"))
    batch_dir = make_batch(root, "batch-a", [0, 1, 2])

    verification = verify_mock_batch(
        batch_dir=batch_dir,
        stdout_json_path=batch_dir / "stdout.json",
        exit_code=0,
    )

    if verification.batch_id != "batch-a":
        raise AssertionError(f"unexpected batch id: {verification.batch_id}")
    if verification.record_count != 3:
        raise AssertionError(f"unexpected record count: {verification.record_count}")
    print("[PASS] single batch verification")


def run_multi_batch_merge() -> None:
    root = Path(tempfile.mkdtemp(prefix="csd-mock-merge-"))
    batch_a = make_batch(root, "batch-a", [0, 1])
    batch_b = make_batch(root, "batch-b", [2, 3])
    merged_path = root / "merged-records.jsonl"

    result = merge_mock_batches(
        batch_dirs=[batch_a, batch_b],
        merged_output_path=merged_path,
        stdout_json_paths=[batch_a / "stdout.json", batch_b / "stdout.json"],
        exit_codes=[0, 0],
    )

    merged_records = [json.loads(line) for line in merged_path.read_text(encoding="utf-8").splitlines()]
    if result["recordCount"] != 4:
        raise AssertionError(f"unexpected merged record count: {result}")
    if [record["index"] for record in merged_records] != [0, 1, 2, 3]:
        raise AssertionError(f"unexpected merged order: {merged_records}")
    print("[PASS] multi batch merge")


def main() -> None:
    run_single_batch_verification()
    run_multi_batch_merge()


if __name__ == "__main__":
    main()
