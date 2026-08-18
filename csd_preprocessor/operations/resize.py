"""Resize operation - image resizing with letterbox padding.

CSD Suitability: GOOD
- Reads large images, outputs smaller images (data reduction)
- Compute: bilinear/area interpolation (moderate)
- Letterbox preserves aspect ratio (critical for YOLO)

References:
- Ultralytics YOLOv5/v8 letterbox implementation
"""

import json
import logging
from pathlib import Path

import cv2
import numpy as np

from .base import BaseOperation, OperationContext
from ..core.parallel import map_images
from ..core.registry import register_operation
from typing import Tuple

logger = logging.getLogger(__name__)


def letterbox_resize(
    image: np.ndarray,
    target_size: Tuple[int, int],
    padding_color: Tuple[int, int, int] = (114, 114, 114),
    interpolation: int = cv2.INTER_AREA,
) -> Tuple[np.ndarray, Tuple[float, float], Tuple[int, int]]:
    """Resize image with letterbox padding (preserves aspect ratio).

    Args:
        image: input BGR image
        target_size: (width, height)
        padding_color: RGB padding color
        interpolation: cv2 interpolation flag

    Returns:
        (resized_image, scale_ratio, padding_offset)
    """
    h, w = image.shape[:2]
    target_w, target_h = target_size

    # Compute scale ratio
    scale = min(target_w / w, target_h / h)
    new_w = int(w * scale)
    new_h = int(h * scale)

    # Resize
    resized = cv2.resize(image, (new_w, new_h), interpolation=interpolation)

    # Compute padding
    dw = target_w - new_w
    dh = target_h - new_h
    top = dh // 2
    bottom = dh - top
    left = dw // 2
    right = dw - left

    # Add padding
    padded = cv2.copyMakeBorder(
        resized, top, bottom, left, right,
        cv2.BORDER_CONSTANT, value=padding_color
    )

    return padded, (scale, scale), (left, top)


def center_crop_resize(
    image: np.ndarray,
    target_size: Tuple[int, int],
    interpolation: int = cv2.INTER_AREA,
) -> np.ndarray:
    """Center crop and resize (for classification tasks)."""
    h, w = image.shape[:2]
    target_w, target_h = target_size

    # Crop to square (center)
    min_dim = min(h, w)
    top = (h - min_dim) // 2
    left = (w - min_dim) // 2
    cropped = image[top:top + min_dim, left:left + min_dim]

    return cv2.resize(cropped, (target_w, target_h), interpolation=interpolation)


INTERP_MAP = {
    "nearest": cv2.INTER_NEAREST,
    "bilinear": cv2.INTER_LINEAR,
    "area": cv2.INTER_AREA,
    "cubic": cv2.INTER_CUBIC,
    "lanczos": cv2.INTER_LANCZOS4,
}


@register_operation("resize")
class ResizeOperation(BaseOperation):
    csd_suitability = "GOOD"
    description = "Image resize with letterbox padding or center crop"

    def execute(self, ctx: OperationContext) -> dict:
        target_size = tuple(self.params.get("target_size", [640, 640]))
        method = self.params.get("method", "letterbox")
        padding_color = tuple(self.params.get("padding_color", [114, 114, 114]))
        interp_name = self.params.get("interpolation", "area")
        interpolation = INTERP_MAP.get(interp_name, cv2.INTER_AREA)

        files = ctx.valid_files
        if not files:
            return {"total": 0, "resized": 0}

        # Create output image directory
        img_out_dir = ctx.output_path / "images"
        img_out_dir.mkdir(parents=True, exist_ok=True)

        tracker = getattr(self, "_tracker", None)
        if tracker:
            tracker.start_stage("resize", total=len(files))

        resized_count = 0
        errors = 0
        original_sizes = []
        resized_files = []

        def _resize_one(rel_path):
            """디코드·리사이즈·기록까지 한 장 단위로 수행 (출력 경로가 파일마다 달라
            병렬 실행이 안전하다). 취합만 아래에서 순서대로 한다."""
            try:
                img = cv2.imread(str(ctx.input_path / rel_path))
                if img is None:
                    return None, None, None, None

                h, w = img.shape[:2]

                if method == "letterbox":
                    result, (sx, sy), (pad_x, pad_y) = letterbox_resize(
                        img, target_size, padding_color, interpolation)
                    tf = {"mode": "letterbox", "scale_x": sx, "scale_y": sy,
                          "pad_x": pad_x, "pad_y": pad_y}
                elif method == "center_crop":
                    result = center_crop_resize(img, target_size, interpolation)
                    # 중앙 크롭은 잘려나간 영역이 있어 좌표 변환이 단순 affine 이 아니다.
                    # 어노테이션 변환기가 이 모드를 인지하고 경고하도록 표시만 남긴다.
                    tf = {"mode": "center_crop"}
                else:
                    result = cv2.resize(img, target_size, interpolation=interpolation)
                    tf = {"mode": "stretch",
                          "scale_x": target_size[0] / w, "scale_y": target_size[1] / h,
                          "pad_x": 0, "pad_y": 0}

                tf.update({"orig_w": w, "orig_h": h,
                           "out_w": target_size[0], "out_h": target_size[1]})

                out_name = Path(rel_path).stem + ".jpg"
                cv2.imwrite(str(img_out_dir / out_name), result,
                            [cv2.IMWRITE_JPEG_QUALITY, 95])
                return out_name, (w, h), tf, None
            except Exception as e:
                return None, None, None, e

        transforms = {}

        for i, (rel_path, (out_name, size, tf, err)) in enumerate(
                zip(files, map_images(files, _resize_one))):
            if err is not None:
                logger.warning(f"Resize error for {rel_path}: {err}")
                errors += 1
            elif out_name is None:
                errors += 1
            else:
                original_sizes.append(size)
                resized_files.append(out_name)
                transforms[Path(out_name).stem] = tf
                resized_count += 1

            if tracker:
                tracker.update_stage("resize", i + 1, errors)

        # Update valid_files to use resized filenames
        ctx.valid_files = resized_files

        # 기하 변환 기록 — 어노테이션 좌표는 원본 해상도 기준이므로, 리사이즈 이후에
        # 라벨을 변환하려면 스케일·패딩을 알아야 한다. stage1 과 stage2 는 별개의
        # 실행이라 컨텍스트로는 전달되지 않으므로 출력 디렉터리에 남긴다.
        ctx.resize_transform = transforms
        try:
            with open(ctx.output_path / "resize_transform.json", "w") as f:
                json.dump({"method": method, "target_size": list(target_size),
                           "images": transforms}, f)
        except Exception as e:
            logger.warning(f"Failed to write resize_transform.json: {e}")

        avg_orig = (
            (np.mean([s[0] for s in original_sizes]), np.mean([s[1] for s in original_sizes]))
            if original_sizes else (0, 0)
        )

        return {
            "total": len(files),
            "resized": resized_count,
            "errors": errors,
            "target_size": list(target_size),
            "method": method,
            "avg_original_size": [int(avg_orig[0]), int(avg_orig[1])],
        }
