"""Quality filtering operation - blur, exposure, resolution checks.

CSD Suitability: GOOD
- Reads full image but performs simple analysis (Laplacian variance, histogram)
- Filters out low-quality data early in the pipeline
- Reduces data volume for subsequent operations
"""

import logging
from pathlib import Path

import cv2
import numpy as np

from .base import BaseOperation, OperationContext
from ..core.parallel import map_images
from ..core.registry import register_operation

logger = logging.getLogger(__name__)


def _to_gray(image: np.ndarray) -> np.ndarray:
    return cv2.cvtColor(image, cv2.COLOR_BGR2GRAY) if len(image.shape) == 3 else image


def compute_blur_score(image: np.ndarray, gray: np.ndarray = None) -> float:
    """Compute blur score using Laplacian variance.

    Higher value = sharper image. Typical threshold: 100.0
    Reference: Pech-Pacheco et al. "Diatom autofocusing in brightfield microscopy"

    분산은 cv2.meanStdDev(C++ 1패스)로 구한다 — numpy .var() 는 평균·편차로 두 번
    훑는다. Laplacian 출력도 CV_64F 대신 CV_32F 면 충분하다(값 범위 ±1020,
    누적은 meanStdDev 내부에서 double). 실측 상대오차 1e-15 수준.
    """
    gray = _to_gray(image) if gray is None else gray
    _, lap_std = cv2.meanStdDev(cv2.Laplacian(gray, cv2.CV_32F))
    return float(lap_std[0, 0]) ** 2


def compute_brightness(image: np.ndarray, gray: np.ndarray = None) -> float:
    """Compute average brightness (0-255)."""
    gray = _to_gray(image) if gray is None else gray
    return float(cv2.meanStdDev(gray)[0][0, 0])


def compute_contrast(image: np.ndarray, gray: np.ndarray = None) -> float:
    """Compute contrast using standard deviation of pixel values."""
    gray = _to_gray(image) if gray is None else gray
    return float(cv2.meanStdDev(gray)[1][0, 0])


def compute_quality_metrics(image: np.ndarray) -> tuple:
    """(blur, brightness, contrast) — 그레이 변환 1회, 통계 1패스로 셋을 함께 구한다.

    이전에는 세 함수가 각자 cvtColor 를 호출해 같은 변환을 이미지당 3번 했다.
    측정(CSD, 0.28MP): 46.1 → 26.5 ms/장.
    """
    gray = _to_gray(image)
    mean, std = cv2.meanStdDev(gray)
    return (compute_blur_score(image, gray), float(mean[0, 0]), float(std[0, 0]))


@register_operation("filter_quality")
class FilterQualityOperation(BaseOperation):
    csd_suitability = "GOOD"
    description = "Quality filtering - blur detection, exposure check, contrast"

    def execute(self, ctx: OperationContext) -> dict:
        min_blur = self.params.get("min_blur_score", 100.0)
        min_brightness = self.params.get("min_brightness", 20)
        max_brightness = self.params.get("max_brightness", 235)
        min_contrast = self.params.get("min_contrast", 10.0)

        files = ctx.valid_files
        if not files:
            return {"total": 0, "passed": 0, "filtered": 0}

        tracker = getattr(self, "_tracker", None)
        if tracker:
            tracker.start_stage("filter_quality", total=len(files))

        passed_files = []
        filtered_files = []
        errors = 0

        def _measure(rel_path):
            try:
                img = cv2.imread(str(ctx.input_path / rel_path))
                if img is None:
                    return None, None
                return compute_quality_metrics(img), None
            except Exception as e:
                return None, e

        measured = map_images(files, _measure)

        for i, (rel_path, (metrics, err)) in enumerate(zip(files, measured)):
            try:
                if err is not None:
                    raise err
                if metrics is None:
                    filtered_files.append({"file": rel_path, "reason": "cannot read"})
                    errors += 1
                    continue

                blur, brightness, contrast = metrics

                reasons = []
                if blur < min_blur:
                    reasons.append(f"blurry (score={blur:.1f} < {min_blur})")
                if brightness < min_brightness:
                    reasons.append(f"too dark (brightness={brightness:.1f})")
                if brightness > max_brightness:
                    reasons.append(f"too bright (brightness={brightness:.1f})")
                if contrast < min_contrast:
                    reasons.append(f"low contrast (std={contrast:.1f})")

                if reasons:
                    filtered_files.append({"file": rel_path, "reason": "; ".join(reasons)})
                else:
                    passed_files.append(rel_path)

            except Exception as e:
                filtered_files.append({"file": rel_path, "reason": str(e)})
                errors += 1

            if tracker:
                tracker.update_stage("filter_quality", i + 1, errors)

        ctx.valid_files = passed_files

        import json
        report_path = ctx.work_dir / "quality_report.json"
        report_path.write_text(json.dumps({
            "total": len(files),
            "passed": len(passed_files),
            "filtered": len(filtered_files),
            "filtered_files": filtered_files[:100],  # limit report size
        }, indent=2), encoding="utf-8")

        return {
            "total": len(files),
            "passed": len(passed_files),
            "filtered": len(filtered_files),
        }
