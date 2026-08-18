#!/usr/bin/env python3
"""CSD Preprocessing CLI.

Usage:
    # Run from job manifest
    ./run_local_python.sh cli.py run --manifest jobs/job_001.json

    # Run from pipeline template
    ./run_local_python.sh cli.py run --template yolo_object_detection --input /data/raw --output /data/preprocessed

    # Check job status (HOST-side)
    ./run_local_python.sh cli.py status --output /data/preprocessed/job_001

    # List available operations
    ./run_local_python.sh cli.py ops

    # Watch for new data (CSD-side daemon)
    ./run_local_python.sh cli.py watch --dir /data/raw --template yolo_object_detection --output /data/preprocessed
"""

import argparse
import json
import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))


def setup_logging(level: str = "INFO"):
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def cmd_run(args):
    """Run a preprocessing pipeline."""
    from csd_preprocessor.core.engine import PreprocessingEngine
    from csd_preprocessor.core.job import Job

    engine = PreprocessingEngine(config_path=args.config)

    if args.manifest:
        result = engine.run_from_manifest(args.manifest)
    elif args.template:
        if not args.input or not args.output:
            print("Error: --input and --output required with --template")
            sys.exit(1)
        result = engine.run_from_template(
            template_name=args.template,
            input_path=args.input,
            output_path=args.output,
            label_path=args.labels or "",
            source_format=args.format or "auto",
        )
    else:
        print("Error: either --manifest or --template required")
        sys.exit(1)

    print(json.dumps(result, indent=2, ensure_ascii=False))
    return result


def cmd_status(args):
    """Check preprocessing job status (HOST-side)."""
    from csd_preprocessor.core.status import StatusTracker

    output_path = Path(args.output)

    if StatusTracker.is_failed(output_path):
        print(f"Status: FAILED")
        failed_path = output_path / "_status" / "FAILED"
        if failed_path.exists():
            print(failed_path.read_text())
    elif StatusTracker.is_completed(output_path):
        print(f"Status: COMPLETED")
    else:
        print(f"Status: IN PROGRESS")

    progress = StatusTracker.read_progress(output_path)
    if progress:
        print(json.dumps(progress, indent=2, ensure_ascii=False))
    else:
        print("No progress file found")


def cmd_ops(args):
    """List available preprocessing operations."""
    # Import to trigger registration
    from csd_preprocessor.core.engine import _ensure_operations_loaded
    from csd_preprocessor.core.registry import list_operations, get_operation

    _ensure_operations_loaded()

    ops = list_operations()
    print(f"\nAvailable Preprocessing Operations ({len(ops)}):")
    print("-" * 60)
    for name in ops:
        op_class = get_operation(name)
        suitability = getattr(op_class, "csd_suitability", "UNKNOWN")
        description = getattr(op_class, "description", "")
        print(f"  {name:<25} [{suitability:<10}] {description}")
    print()


def cmd_watch(args):
    """Watch for new data and auto-process (CSD daemon mode)."""
    from csd_preprocessor.core.engine import PreprocessingEngine
    from csd_preprocessor.io.watcher import DataWatcher

    engine = PreprocessingEngine(config_path=args.config)

    def on_new_data(new_files):
        logging.info(f"New data detected: {len(new_files)} files")
        try:
            result = engine.run_from_template(
                template_name=args.template,
                input_path=args.dir,
                output_path=args.output,
                source_format=args.format or "auto",
            )
            logging.info(f"Pipeline result: {result.get('status', 'unknown')}")
        except Exception as e:
            logging.error(f"Pipeline error: {e}")

    watcher = DataWatcher(
        watch_dir=args.dir,
        poll_interval=args.interval,
    )

    print(f"Watching {args.dir} for new data...")
    print(f"Template: {args.template}")
    print(f"Output: {args.output}")
    print("Press Ctrl+C to stop")

    try:
        watcher.watch(on_new_data)
    except KeyboardInterrupt:
        print("\nStopping watcher...")
        watcher.stop()


def cmd_mock_verify(args):
    """Verify and optionally merge mock worker batch outputs (HOST-side)."""
    from csd_preprocessor.io.mock_batch import merge_mock_batches

    result = merge_mock_batches(
        batch_dirs=args.batch_dir,
        merged_output_path=args.merged_output,
        stdout_json_paths=args.stdout_json,
        exit_codes=args.exit_code,
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return result


def main():
    parser = argparse.ArgumentParser(
        description="CSD-Based AI Training Data Preprocessor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--config", help="Path to config YAML", default=None)
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # run command
    run_parser = subparsers.add_parser("run", help="Run preprocessing pipeline")
    run_parser.add_argument("--manifest", help="Path to job manifest JSON")
    run_parser.add_argument("--template", help="Pipeline template name")
    run_parser.add_argument("--input", help="Input data directory")
    run_parser.add_argument("--output", help="Output directory")
    run_parser.add_argument("--labels", help="Label/annotation directory")
    run_parser.add_argument("--format", help="Source annotation format (auto, coco, voc, yolo)")

    # status command
    status_parser = subparsers.add_parser("status", help="Check job status (HOST-side)")
    status_parser.add_argument("--output", required=True, help="Output directory to check")

    # ops command
    subparsers.add_parser("ops", help="List available operations")

    # watch command
    watch_parser = subparsers.add_parser("watch", help="Watch for new data (CSD daemon)")
    watch_parser.add_argument("--dir", required=True, help="Directory to watch")
    watch_parser.add_argument("--template", required=True, help="Pipeline template to run")
    watch_parser.add_argument("--output", required=True, help="Output directory")
    watch_parser.add_argument("--format", help="Source annotation format")
    watch_parser.add_argument("--interval", type=float, default=5.0, help="Poll interval (seconds)")

    # mock-verify command
    verify_parser = subparsers.add_parser(
        "mock-verify",
        help="Verify mock worker outputs and merge records.jsonl files (HOST-side)",
    )
    verify_parser.add_argument(
        "--batch-dir",
        action="append",
        required=True,
        help="Batch output directory containing result.json and records.jsonl. Repeat for multiple batches.",
    )
    verify_parser.add_argument(
        "--merged-output",
        required=True,
        help="Path to write merged records.jsonl",
    )
    verify_parser.add_argument(
        "--stdout-json",
        action="append",
        help="Optional path to captured stdout JSON for each batch. Repeat in the same order as --batch-dir.",
    )
    verify_parser.add_argument(
        "--exit-code",
        action="append",
        type=int,
        help="Optional expected exit code for each batch. Repeat in the same order as --batch-dir.",
    )

    args = parser.parse_args()
    setup_logging(args.log_level)

    if args.command == "run":
        cmd_run(args)
    elif args.command == "status":
        cmd_status(args)
    elif args.command == "ops":
        cmd_ops(args)
    elif args.command == "watch":
        cmd_watch(args)
    elif args.command == "mock-verify":
        cmd_mock_verify(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
