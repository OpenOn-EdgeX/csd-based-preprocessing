"""Host-side verification helpers for mock worker batch outputs."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class MockBatchVerification:
    batch_dir: Path
    batch_id: str
    record_count: int
    result: dict[str, Any]
    records_path: Path
    result_path: Path


def _load_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return data


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line_no, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not raw.strip():
            continue
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError(f"{path}:{line_no} must contain a JSON object per line")
        records.append(data)
    return records


def verify_mock_batch(
    batch_dir: str | Path,
    stdout_json_path: str | Path | None = None,
    exit_code: int | None = None,
) -> MockBatchVerification:
    batch_dir = Path(batch_dir)
    records_path = batch_dir / "records.jsonl"
    result_path = batch_dir / "result.json"

    if not records_path.exists():
        raise FileNotFoundError(f"Missing records file: {records_path}")
    if not result_path.exists():
        raise FileNotFoundError(f"Missing result file: {result_path}")

    result = _load_json(result_path)
    records = _load_jsonl(records_path)

    batch_id = str(result.get("batchId", "")).strip()
    if not batch_id:
        raise ValueError(f"{result_path} missing non-empty batchId")

    input_count = result.get("inputCount")
    output_count = result.get("outputCount")
    if not isinstance(input_count, int) or input_count < 0:
        raise ValueError(f"{result_path} has invalid inputCount")
    if not isinstance(output_count, int) or output_count < 0:
        raise ValueError(f"{result_path} has invalid outputCount")

    if output_count != len(records):
        raise ValueError(
            f"{result_path} outputCount={output_count} does not match records.jsonl lines={len(records)}"
        )
    if input_count != len(records):
        raise ValueError(
            f"{result_path} inputCount={input_count} does not match records.jsonl lines={len(records)}"
        )

    output_file = str(result.get("outputFile", "")).strip()
    if not output_file:
        raise ValueError(f"{result_path} missing outputFile")
    if Path(output_file).name != "records.jsonl":
        raise ValueError(f"{result_path} outputFile must point to records.jsonl")

    seen_indexes: set[int] = set()
    for line_no, record in enumerate(records, start=1):
        index = record.get("index")
        queue_position = record.get("queuePosition")
        if not isinstance(index, int):
            raise ValueError(f"{records_path}:{line_no} missing integer index")
        if not isinstance(queue_position, int):
            raise ValueError(f"{records_path}:{line_no} missing integer queuePosition")
        if index in seen_indexes:
            raise ValueError(f"{records_path}:{line_no} duplicate index {index}")
        seen_indexes.add(index)

    if stdout_json_path is not None:
        stdout_data = _load_json(Path(stdout_json_path))
        if stdout_data != result:
            raise ValueError("stdout JSON does not exactly match result.json")

    if exit_code is not None and exit_code != 0:
        raise ValueError(f"Job exit code must be 0, got {exit_code}")

    return MockBatchVerification(
        batch_dir=batch_dir,
        batch_id=batch_id,
        record_count=len(records),
        result=result,
        records_path=records_path,
        result_path=result_path,
    )


def merge_mock_batches(
    batch_dirs: list[str | Path],
    merged_output_path: str | Path,
    stdout_json_paths: list[str | Path] | None = None,
    exit_codes: list[int] | None = None,
) -> dict[str, Any]:
    if not batch_dirs:
        raise ValueError("At least one batch directory is required")

    stdout_json_paths = stdout_json_paths or []
    exit_codes = exit_codes or []
    if stdout_json_paths and len(stdout_json_paths) != len(batch_dirs):
        raise ValueError("stdout JSON path count must match batch directory count")
    if exit_codes and len(exit_codes) != len(batch_dirs):
        raise ValueError("exit code count must match batch directory count")

    verifications: list[MockBatchVerification] = []
    merged_records: list[dict[str, Any]] = []

    for idx, batch_dir in enumerate(batch_dirs):
        verification = verify_mock_batch(
            batch_dir=batch_dir,
            stdout_json_path=stdout_json_paths[idx] if stdout_json_paths else None,
            exit_code=exit_codes[idx] if exit_codes else None,
        )
        verifications.append(verification)
        merged_records.extend(_load_jsonl(verification.records_path))

    merged_output_path = Path(merged_output_path)
    merged_output_path.parent.mkdir(parents=True, exist_ok=True)
    with merged_output_path.open("w", encoding="utf-8") as fp:
        for record in merged_records:
            fp.write(json.dumps(record, ensure_ascii=False) + "\n")

    return {
        "status": "completed",
        "batchCount": len(verifications),
        "batchIds": [verification.batch_id for verification in verifications],
        "recordCount": len(merged_records),
        "mergedOutputFile": str(merged_output_path),
    }
