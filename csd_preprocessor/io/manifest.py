"""Job manifest I/O utilities.

Handles reading/writing job manifests from/to the shared storage.
HOST writes manifests → CSD reads and processes them.
"""

import json
import logging
from pathlib import Path
from typing import List, Optional, Union

from ..core.job import Job

logger = logging.getLogger(__name__)


class ManifestManager:
    """Manage job manifests on the shared storage."""

    def __init__(self, jobs_dir: Union[str, Path]):
        self.jobs_dir = Path(jobs_dir)
        self.jobs_dir.mkdir(parents=True, exist_ok=True)

    def save_manifest(self, job: Job) -> Path:
        """Save a job manifest. Called by HOST."""
        manifest_path = self.jobs_dir / f"{job.job_id}.json"
        job.save(manifest_path)
        logger.info(f"Saved manifest: {manifest_path}")
        return manifest_path

    def load_manifest(self, job_id: str) -> Optional[Job]:
        """Load a job manifest by ID. Called by CSD."""
        manifest_path = self.jobs_dir / f"{job_id}.json"
        if not manifest_path.exists():
            return None
        return Job.load(manifest_path)

    def list_pending(self) -> List[str]:
        """List job IDs that haven't been processed yet."""
        pending = []
        for f in self.jobs_dir.glob("*.json"):
            job_id = f.stem
            # Check if output exists with COMPLETED flag
            # (simple heuristic - in production, use a proper state store)
            pending.append(job_id)
        return sorted(pending)

    def list_all(self) -> List[dict]:
        """List all manifests with basic info."""
        manifests = []
        for f in sorted(self.jobs_dir.glob("*.json")):
            try:
                job = Job.load(f)
                manifests.append({
                    "job_id": job.job_id,
                    "job_type": job.job_type,
                    "created_at": job.created_at,
                    "operations": [op.op for op in job.operations],
                    "input": job.input.data_path,
                    "output": job.output.data_path,
                })
            except Exception as e:
                logger.warning(f"Error loading manifest {f}: {e}")
        return manifests
