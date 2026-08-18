"""Normalize operation - compute per-channel mean and std.

CSD Suitability: EXCELLENT
- Reads every pixel of every image (massive I/O)
- Outputs only 6 numbers (3 means + 3 stds) → extreme data reduction
- Reference: Summarizer (MICRO 2022) - in-storage statistics computation
"""

import logging
from pathlib import Path

import cv2
import numpy as np

from .base import BaseOperation, OperationContext
from ..core.parallel import map_images
from ..core.registry import register_operation

logger = logging.getLogger(__name__)


@register_operation("normalize")
class NormalizeOperation(BaseOperation):
    csd_suitability = "EXCELLENT"
    description = "Compute per-channel mean and standard deviation for normalization"

    def execute(self, ctx: OperationContext) -> dict:
        compute_mean = self.params.get("per_channel_mean", True)
        compute_std = self.params.get("per_channel_std", True)

        files = ctx.valid_files
        if not files:
            return {"mean": [0, 0, 0], "std": [1, 1, 1]}

        tracker = getattr(self, "_tracker", None)
        if tracker:
            tracker.start_stage("normalize", total=len(files))

        # 채널별 합/제곱합만 누적한다 — 이미지를 한 장씩 처리하므로 데이터셋
        # 전체를 메모리에 올리지 않는다. 전역 mean/std 는 루프 뒤에서 한 번 계산.
        n = 0
        channel_sum = np.zeros(3, dtype=np.float64)
        channel_sum_sq = np.zeros(3, dtype=np.float64)
        pixel_count = 0

        # Check if images are in output (resized) or input (original)
        img_dir = ctx.output_path / "images"
        if not img_dir.exists():
            img_dir = ctx.input_path

        def _stats_one(rel_path):
            """한 장의 (평균, 표준편차, 픽셀수). 병렬 실행되므로 누적은 하지 않는다."""
            fpath = img_dir / rel_path
            if not fpath.exists():
                fpath = ctx.input_path / rel_path
            if not fpath.exists():
                return None
            try:
                img = cv2.imread(str(fpath))
                if img is None:
                    return None

                # 채널별 합/제곱합을 cv2.meanStdDev(C++ 1패스)로 구한다.
                # 이전 구현은 이미지마다 float64 배열(640x640x3 = 9.8MB)을 만들고
                # 제곱 배열을 또 만들어 5번 넘게 훑었다 — CSD(ARM)에서 파이프라인
                # 전체 시간의 57%를 차지했다. 산술적으로는 동일하다:
                #   sum   = mean * p
                #   sumsq = (std^2 + mean^2) * p      (std 는 모표준편차)
                # 측정: CSD 155.6 → 18.9 ms/장, CPU 30.2 → 3.4 ms/장, 결과값 동일.
                mean_bgr, std_bgr = cv2.meanStdDev(img)
                mean_ch = mean_bgr.ravel() / 255.0
                std_ch = std_bgr.ravel() / 255.0
                if mean_ch.size == 1:                      # 흑백 → 3채널로 복제
                    mean_ch = np.repeat(mean_ch, 3)
                    std_ch = np.repeat(std_ch, 3)
                elif mean_ch.size < 3:
                    logger.warning(f"Normalize: unexpected channel count for {rel_path}")
                    return None
                # cv2 는 BGR 순서로 돌려주므로 RGB 로 뒤집는다 (이전 cvtColor 와 동일)
                h, w = img.shape[:2]
                return mean_ch[2::-1], std_ch[2::-1], h * w
            except Exception as e:
                logger.warning(f"Normalize error for {rel_path}: {e}")
                return None

        # 누적은 반드시 순차로 — 부동소수 덧셈은 순서에 따라 결과가 달라진다.
        for i, stats in enumerate(map_images(files, _stats_one)):
            if stats is not None:
                mean_ch, std_ch, pixels = stats
                channel_sum += mean_ch * pixels
                channel_sum_sq += (std_ch ** 2 + mean_ch ** 2) * pixels
                pixel_count += pixels
                n += 1

            if tracker:
                tracker.update_stage("normalize", i + 1)

        if pixel_count == 0:
            mean = [0.0, 0.0, 0.0]
            std = [1.0, 1.0, 1.0]
        else:
            mean = (channel_sum / pixel_count).tolist()
            variance = (channel_sum_sq / pixel_count) - np.array(mean) ** 2
            std = np.sqrt(np.maximum(variance, 0)).tolist()

        result = {
            "mean": [round(m, 6) for m in mean],
            "std": [round(s, 6) for s in std],
            "images_processed": n,
            "total_pixels": pixel_count,
        }

        ctx.statistics["normalization"] = result
        return result
