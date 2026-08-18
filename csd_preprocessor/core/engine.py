"""Pipeline engine - the main orchestrator for CSD preprocessing.

Reads a Job manifest, executes operations in sequence,
tracks progress via file-based status, and writes results
to the shared storage for HOST to consume.

Output directory policy:
  HOST가 보는 최종 디렉토리에는 학습에 필요한 파일만 남긴다.
  중간 작업물(_work/, augmented_images/ 등)은 파이프라인 완료 후 제거.
"""

import logging
import shutil
import time
from pathlib import Path
from typing import Optional, Union

import yaml

from .job import Job
from .registry import get_operation
from .status import StatusTracker
from ..operations.base import OperationContext

logger = logging.getLogger(__name__)


class PreprocessingEngine:
    """Main preprocessing pipeline engine.

    This runs on the CSD side:
    1. Read job manifest from jobs/ directory
    2. Execute preprocessing operations in sequence
    3. Write progress to _status/ directory
    4. Write final results to preprocessed/ directory
    5. HOST reads results from preprocessed/ directory
    """

    def __init__(self, config_path: Optional[Union[str, Path]] = None):
        self.config = self._load_config(config_path)

    def _load_config(self, config_path: Optional[Union[str, Path]]) -> dict:
        if config_path is None:
            config_path = Path(__file__).parent.parent.parent / "config" / "default.yaml"
        config_path = Path(config_path)
        if config_path.exists():
            with open(config_path, "r") as f:
                return yaml.safe_load(f)
        return {}

    def run_job(self, job: Job) -> dict:
        """Execute a complete preprocessing job.

        Args:
            job: Job manifest to execute

        Returns:
            dict with job results and metrics
        """
        logger.info(f"Starting job: {job.job_id} ({job.job_type})")
        start_time = time.time()

        # Ensure output directory exists
        output_path = Path(job.output.data_path)
        output_path.mkdir(parents=True, exist_ok=True)

        # Initialize status tracker (CSD writes, HOST reads)
        tracker = StatusTracker(output_path)
        stage_names = [op.op for op in job.operations]
        tracker.init_job(job.job_id, stage_names)

        # Build operation context
        ctx = OperationContext(
            input_path=Path(job.input.data_path),
            output_path=output_path,
            label_path=Path(job.input.label_path) if job.input.label_path else None,
            source_format=job.input.format,
        )

        results = {}
        failed_stages = []

        try:
            # Execute operations in sequence
            for op_spec in job.operations:
                op_name = op_spec.op
                op_params = op_spec.params

                logger.info(f"Executing operation: {op_name}")
                tracker.start_stage(op_name)

                try:
                    # Import all operations to ensure registration
                    _ensure_operations_loaded()

                    # Get operation class from registry
                    op_class = get_operation(op_name)
                    op_instance = op_class(params=op_params)

                    # Validate parameters
                    errors = op_instance.validate_params()
                    if errors:
                        tracker.fail_stage(op_name, f"Invalid params: {errors}")
                        failed_stages.append(op_name)
                        continue

                    # Execute with progress tracking
                    op_instance._tracker = tracker
                    op_result = op_instance.execute(ctx)

                    results[op_name] = op_result
                    tracker.complete_stage(
                        op_name,
                        message=f"Completed successfully",
                        metrics=op_result,
                    )

                except Exception as e:
                    error_msg = f"{type(e).__name__}: {e}"
                    logger.error(f"Operation {op_name} failed: {error_msg}")
                    tracker.fail_stage(op_name, error_msg)
                    results[op_name] = {"error": error_msg}
                    failed_stages.append(op_name)

            # Generate data.yaml if requested
            if job.output.generate_data_yaml and job.output.structure == "yolo":
                self._generate_data_yaml(ctx, output_path)

            # Clean up intermediate artifacts
            # HOST가 보는 디렉토리에는 학습에 필요한 파일만 남긴다
            self._cleanup_intermediates(output_path)

            elapsed = time.time() - start_time
            if failed_stages:
                error_msg = f"One or more stages failed: {', '.join(failed_stages)}"
                tracker.fail_job(error_msg)
                logger.error(f"Job {job.job_id} failed in {elapsed:.1f}s: {error_msg}")
                return {
                    "job_id": job.job_id,
                    "status": "failed",
                    "error": error_msg,
                    "elapsed_seconds": round(elapsed, 2),
                    "results": results,
                }

            tracker.complete_job()
            logger.info(f"Job {job.job_id} completed in {elapsed:.1f}s")

            return {
                "job_id": job.job_id,
                "status": "completed",
                "elapsed_seconds": round(elapsed, 2),
                "results": results,
            }

        except Exception as e:
            error_msg = f"{type(e).__name__}: {e}"
            logger.error(f"Job {job.job_id} failed: {error_msg}")
            tracker.fail_job(error_msg)
            return {
                "job_id": job.job_id,
                "status": "failed",
                "error": error_msg,
                "results": results,
            }

    def run_from_manifest(self, manifest_path: Union[str, Path]) -> dict:
        """Load and execute a job from a manifest file."""
        job = Job.load(manifest_path)
        return self.run_job(job)

    def run_from_template(
        self,
        template_name: str,
        input_path: str,
        output_path: str,
        label_path: str = "",
        source_format: str = "auto",
    ) -> dict:
        """Run a pipeline from a template name."""
        template_dir = Path(__file__).parent.parent.parent / "config" / "pipeline_templates"
        template_path = template_dir / f"{template_name}.yaml"
        if not template_path.exists():
            available = [f.stem for f in template_dir.glob("*.yaml")]
            raise FileNotFoundError(
                f"Template '{template_name}' not found. Available: {available}"
            )
        job = Job.from_pipeline_template(
            template_path, input_path, output_path, label_path, source_format
        )
        return self.run_job(job)

    def _cleanup_intermediates(self, output_path: Path):
        """Remove intermediate work directories from final output.

        Stage 1 (cleaned/) 최종 구조:
          cleaned/
          ├── images/        ← KEEP (정제된 이미지, HOST가 라벨링에 사용)
          └── _status/

        Stage 2 (preprocessed/) 최종 구조:
          preprocessed/
          ├── train/images/, train/labels/
          ├── val/images/, val/labels/
          ├── test/images/, test/labels/
          ├── data.yaml
          ├── statistics.json
          └── _status/
        """
        # _work/ 는 항상 제거 (중간 리포트, augmented 임시 파일)
        work_dir = output_path / "_work"
        if work_dir.exists():
            shutil.rmtree(work_dir)
            logger.info("Cleaned up intermediate directory: _work/")

        # split이 완료된 경우 (train/ 존재), 최상위 images/는 불필요
        # split이 없는 경우 (Stage 1), images/는 최종 출력물이므로 유지
        has_splits = (output_path / "train").exists()
        if has_splits:
            top_images = output_path / "images"
            if top_images.exists():
                shutil.rmtree(top_images)
                logger.info("Cleaned up intermediate directory: images/")

    def _generate_data_yaml(self, ctx: OperationContext, output_path: Path):
        """Generate YOLO data.yaml for training.

        path를 "." (상대경로)로 설정하여 HOST가 어디에 마운트하든 동작.
        예: /mnt/nvme2n1p3/preprocessed/ 에서 data.yaml을 읽으면
            train: ./train/images/ 로 해석됨
        """
        data = {
            "path": ".",
            "train": "train/images",
            "val": "val/images",
            "test": "test/images",
        }

        if ctx.class_names:
            data["nc"] = len(ctx.class_names)
            data["names"] = ctx.class_names
        elif ctx.class_mapping:
            names_by_id = {v: k for k, v in ctx.class_mapping.items()}
            data["nc"] = len(names_by_id)
            data["names"] = [names_by_id[i] for i in range(len(names_by_id))]

        yaml_path = output_path / "data.yaml"
        with open(yaml_path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, allow_unicode=True)
        logger.info(f"Generated data.yaml at {yaml_path}")


def _ensure_operations_loaded():
    """Import all operation modules to trigger registration."""
    from ..operations import (  # noqa: F401
        validate,
        deduplicate,
        filter_quality,
        convert_annotation,
        resize,
        normalize,
        augment,
        split,
        statistics,
        tile,
    )
