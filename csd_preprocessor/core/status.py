"""File-based status tracking for CSD preprocessing.

The CSD writes status files that the HOST can poll to monitor progress.
Key pattern:
- _status/progress.json  → updated periodically during processing
- _status/COMPLETED      → empty flag file, existence = job completed
- _status/FAILED         → contains error details if job failed
"""

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union

logger = logging.getLogger(__name__)


@dataclass
class StageStatus:
    """Status of a single preprocessing stage."""
    name: str
    status: str = "pending"  # pending, in_progress, completed, failed, skipped
    processed: int = 0
    total: int = 0
    errors: int = 0
    message: str = ""
    started_at: str = ""
    completed_at: str = ""
    metrics: dict = field(default_factory=dict)

    def start(self, total: int = 0):
        self.status = "in_progress"
        self.total = total
        self.started_at = datetime.now().isoformat()

    def update(self, processed: int, errors: int = 0):
        self.processed = processed
        self.errors = errors

    def complete(self, message: str = ""):
        self.status = "completed"
        self.message = message
        self.completed_at = datetime.now().isoformat()

    def fail(self, message: str):
        self.status = "failed"
        self.message = message
        self.completed_at = datetime.now().isoformat()

    def skip(self, message: str = ""):
        self.status = "skipped"
        self.message = message


class StatusTracker:
    """File-based status tracker.

    Writes progress to _status/ directory in the output path.
    HOST can read these files to monitor CSD preprocessing progress.
    """

    def __init__(self, output_path: Union[str, Path]):
        self.output_path = Path(output_path)
        self.status_dir = self.output_path / "_status"
        self.status_dir.mkdir(parents=True, exist_ok=True)

        self.job_id: str = ""
        self.stages: Dict[str, StageStatus] = {}
        self.current_stage: Optional[str] = None
        self.start_time: str = ""
        self._stage_order: List[str] = []

    def init_job(self, job_id: str, stage_names: List[str]):
        """Initialize tracking for a new job."""
        self.job_id = job_id
        self.start_time = datetime.now().isoformat()
        self._stage_order = stage_names
        self.stages = {name: StageStatus(name=name) for name in stage_names}
        # Remove any leftover completion flags
        for f in ["COMPLETED", "FAILED"]:
            flag = self.status_dir / f
            if flag.exists():
                flag.unlink()
        self._write_progress()

    def start_stage(self, name: str, total: int = 0):
        """Mark a stage as started."""
        if name in self.stages:
            self.current_stage = name
            self.stages[name].start(total)
            self._write_progress()
            logger.info(f"[{self.job_id}] Stage '{name}' started (total={total})")

    def update_stage(self, name: str, processed: int, errors: int = 0):
        """Update stage progress."""
        if name in self.stages:
            self.stages[name].update(processed, errors)
            # Write progress every 50 items to avoid excessive I/O
            if processed % 50 == 0 or processed == self.stages[name].total:
                self._write_progress()

    def complete_stage(self, name: str, message: str = "", metrics: dict = None):
        """Mark a stage as completed."""
        if name in self.stages:
            self.stages[name].complete(message)
            if metrics:
                self.stages[name].metrics = metrics
            self._write_progress()
            logger.info(f"[{self.job_id}] Stage '{name}' completed: {message}")

    def fail_stage(self, name: str, message: str):
        """Mark a stage as failed."""
        if name in self.stages:
            self.stages[name].fail(message)
            self._write_progress()
            logger.error(f"[{self.job_id}] Stage '{name}' failed: {message}")

    def skip_stage(self, name: str, message: str = ""):
        """Mark a stage as skipped."""
        if name in self.stages:
            self.stages[name].skip(message)
            self._write_progress()

    def complete_job(self):
        """Mark the entire job as completed. Write COMPLETED flag file."""
        self._write_progress()
        failed_flag = self.status_dir / "FAILED"
        if failed_flag.exists():
            failed_flag.unlink()
        flag = self.status_dir / "COMPLETED"
        flag.write_text(datetime.now().isoformat())
        logger.info(f"[{self.job_id}] Job completed")

    def fail_job(self, error: str):
        """Mark the entire job as failed. Write FAILED flag file."""
        self._write_progress()
        completed_flag = self.status_dir / "COMPLETED"
        if completed_flag.exists():
            completed_flag.unlink()
        flag = self.status_dir / "FAILED"
        flag.write_text(json.dumps({
            "job_id": self.job_id,
            "error": error,
            "timestamp": datetime.now().isoformat(),
        }, indent=2))
        logger.error(f"[{self.job_id}] Job failed: {error}")

    def _write_progress(self):
        """Write current progress to progress.json."""
        progress = {
            "job_id": self.job_id,
            "current_stage": self.current_stage,
            "start_time": self.start_time,
            "last_updated": datetime.now().isoformat(),
            "stages": {
                name: asdict(self.stages[name])
                for name in self._stage_order
                if name in self.stages
            },
        }
        progress_file = self.status_dir / "progress.json"
        progress_file.write_text(
            json.dumps(progress, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    @staticmethod
    def is_completed(output_path: Union[str, Path]) -> bool:
        """Check if a job has completed (HOST-side check)."""
        return (Path(output_path) / "_status" / "COMPLETED").exists()

    @staticmethod
    def is_failed(output_path: Union[str, Path]) -> bool:
        """Check if a job has failed (HOST-side check)."""
        return (Path(output_path) / "_status" / "FAILED").exists()

    @staticmethod
    def read_progress(output_path: Union[str, Path]) -> Optional[dict]:
        """Read current progress (HOST-side check)."""
        progress_file = Path(output_path) / "_status" / "progress.json"
        if progress_file.exists():
            return json.loads(progress_file.read_text(encoding="utf-8"))
        return None
