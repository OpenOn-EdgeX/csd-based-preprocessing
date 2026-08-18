#!/usr/bin/env python3
"""Mock Kubernetes worker for shard-level CSD jobs.

This entrypoint consumes shard metadata from environment variables and writes
records.jsonl/result.json in the format expected by the master-side merger.
"""

import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List


def _fail(message: str, exit_code: int = 1) -> None:
    print(message, file=sys.stderr)
    sys.exit(exit_code)


def _require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        _fail(f"Missing required environment variable: {name}")
    return value


def _parse_manifest(raw: str) -> Dict[str, Any]:
    try:
        manifest = json.loads(raw)
    except json.JSONDecodeError as exc:
        _fail(f"Invalid BATCH_MANIFEST_JSON: {exc}")

    if not isinstance(manifest, dict):
        _fail("BATCH_MANIFEST_JSON must decode to a JSON object")
    return manifest


def _resolve_indexes(manifest: Dict[str, Any]) -> List[int]:
    indexes = manifest.get("indexes")
    if indexes is not None:
        if not isinstance(indexes, list) or any(not isinstance(x, int) for x in indexes):
            _fail("Manifest field 'indexes' must be a list of integers")
        return indexes

    start = manifest.get("startIndex")
    end = manifest.get("endIndex")
    if isinstance(start, int) and isinstance(end, int):
        step = -1 if start >= end else 1
        return list(range(start, end + step, step))

    item_count = manifest.get("itemCount", 0)
    if isinstance(item_count, int) and item_count >= 0:
        return list(range(item_count))

    _fail("Manifest must contain 'indexes' or a valid start/end range")
    return []


def _atomic_write(path: Path, content: str) -> None:
    tmp_path = path.with_name(f"{path.name}.tmp")
    tmp_path.write_text(content, encoding="utf-8")
    tmp_path.replace(path)


def _build_records(indexes: Iterable[int], worker: str) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    for queue_position, index in enumerate(indexes):
        records.append(
            {
                "index": index,
                "queuePosition": queue_position,
                "value": f"mock-result-{index}",
                "worker": worker,
            }
        )
    return records


def _write_outputs(output_dir: Path, records: List[Dict[str, Any]], result: Dict[str, Any]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    records_path = output_dir / "records.jsonl"
    result_path = output_dir / "result.json"

    jsonl = "\n".join(json.dumps(record, ensure_ascii=False) for record in records)
    if jsonl:
        jsonl += "\n"
    _atomic_write(records_path, jsonl)
    _atomic_write(result_path, json.dumps(result, indent=2, ensure_ascii=False) + "\n")


def _write_termination_log(result: Dict[str, Any]) -> None:
    termination_log = Path("/dev/termination-log")
    if not termination_log.exists():
        return
    try:
        _atomic_write(termination_log, json.dumps(result, ensure_ascii=False) + "\n")
    except OSError:
        # Non-fatal on platforms where the termination log is not writable.
        pass


def main() -> int:
    started = time.perf_counter_ns()

    manifest = _parse_manifest(_require_env("BATCH_MANIFEST_JSON"))
    output_dir = Path(_require_env("OUTPUT_DIR"))
    _require_env("DATA_PATH")
    output_host_dir = os.environ.get("OUTPUT_HOST_DIR", "").strip()

    batch_id = os.environ.get("BATCH_ID", "").strip() or str(manifest.get("batchId", "")).strip()
    if not batch_id:
        _fail("BATCH_ID or manifest.batchId must be provided")

    worker = os.environ.get("WORKER_TYPE", "").strip() or str(manifest.get("worker", "")).strip() or "CSD"
    indexes = _resolve_indexes(manifest)
    records = _build_records(indexes, worker)

    finished = time.perf_counter_ns()
    duration_nanos = finished - started
    duration_millis = duration_nanos // 1_000_000
    throughput = (len(records) / (duration_nanos / 1_000_000_000)) if duration_nanos > 0 else 0.0
    records_path = output_dir / "records.jsonl"
    reported_records_path = Path(output_host_dir) / "records.jsonl" if output_host_dir else records_path

    result = {
        "batchId": batch_id,
        "worker": worker,
        "backend": "csd",
        "inputCount": len(indexes),
        "outputCount": len(records),
        "durationMillis": duration_millis,
        "durationNanos": duration_nanos,
        "throughputPerSec": round(throughput, 2),
        "outputFile": str(reported_records_path),
    }

    _write_outputs(output_dir, records, result)
    _write_termination_log(result)
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
