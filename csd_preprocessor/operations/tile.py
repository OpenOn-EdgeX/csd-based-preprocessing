"""Tiling operation - split high-resolution images into tiles.

CSD Suitability: EXCELLENT
- Reads 1 large image, outputs many small tiles
- Ideal for satellite imagery, microscopy, high-res surveillance
- Annotations are split and adjusted per tile

Reference:
- SAHI (Slicing Aided Hyper Inference) - Akyon et al. (2022)
"""

import logging
from pathlib import Path

import cv2
import numpy as np

from .base import BaseOperation, OperationContext
from ..core.registry import register_operation
from typing import List, Tuple

logger = logging.getLogger(__name__)


def tile_image(
    image: np.ndarray,
    tile_size: Tuple[int, int],
    overlap: float = 0.2,
) -> List[Tuple[np.ndarray, Tuple[int, int, int, int]]]:
    """Split image into overlapping tiles.

    Args:
        image: input image
        tile_size: (width, height) of each tile
        overlap: overlap ratio between tiles (0.0 - 0.5)

    Returns:
        list of (tile_image, (x_offset, y_offset, tile_w, tile_h))
    """
    h, w = image.shape[:2]
    tw, th = tile_size
    stride_x = int(tw * (1 - overlap))
    stride_y = int(th * (1 - overlap))

    tiles = []
    for y in range(0, h, stride_y):
        for x in range(0, w, stride_x):
            x2 = min(x + tw, w)
            y2 = min(y + th, h)
            x1 = max(0, x2 - tw)
            y1 = max(0, y2 - th)

            tile = image[y1:y2, x1:x2]

            # Pad if tile is smaller than target
            if tile.shape[0] < th or tile.shape[1] < tw:
                padded = np.full((th, tw, 3), 114, dtype=np.uint8)
                padded[:tile.shape[0], :tile.shape[1]] = tile
                tile = padded

            tiles.append((tile, (x1, y1, x2 - x1, y2 - y1)))

    return tiles


def adjust_labels_for_tile(
    labels: List[dict],
    tile_offset: Tuple[int, int, int, int],
    image_size: Tuple[int, int],
    min_visibility: float = 0.3,
) -> List[dict]:
    """Adjust YOLO labels for a tile.

    Args:
        labels: original YOLO labels (normalized coords)
        tile_offset: (x_offset, y_offset, tile_w, tile_h) in pixels
        image_size: (original_w, original_h)
        min_visibility: minimum fraction of bbox that must be visible in tile

    Returns:
        adjusted labels in normalized tile coordinates
    """
    tx, ty, tw, th = tile_offset
    img_w, img_h = image_size
    adjusted = []

    for label in labels:
        # Convert from normalized image coords to pixel coords
        cx_px = label["cx"] * img_w
        cy_px = label["cy"] * img_h
        bw_px = label["w"] * img_w
        bh_px = label["h"] * img_h

        # Bbox in pixel coords
        x1 = cx_px - bw_px / 2
        y1 = cy_px - bh_px / 2
        x2 = cx_px + bw_px / 2
        y2 = cy_px + bh_px / 2

        # Clip to tile
        cx1 = max(x1, tx)
        cy1 = max(y1, ty)
        cx2 = min(x2, tx + tw)
        cy2 = min(y2, ty + th)

        # Check visibility
        if cx2 <= cx1 or cy2 <= cy1:
            continue

        clipped_area = (cx2 - cx1) * (cy2 - cy1)
        original_area = bw_px * bh_px
        if original_area <= 0 or clipped_area / original_area < min_visibility:
            continue

        # Convert to normalized tile coordinates
        ncx = ((cx1 + cx2) / 2 - tx) / tw
        ncy = ((cy1 + cy2) / 2 - ty) / th
        nw = (cx2 - cx1) / tw
        nh = (cy2 - cy1) / th

        adjusted.append({
            "class_id": label["class_id"],
            "cx": max(0, min(1, ncx)),
            "cy": max(0, min(1, ncy)),
            "w": max(0, min(1, nw)),
            "h": max(0, min(1, nh)),
        })

    return adjusted


@register_operation("tile")
class TileOperation(BaseOperation):
    csd_suitability = "EXCELLENT"
    description = "Split high-resolution images into overlapping tiles"

    def execute(self, ctx: OperationContext) -> dict:
        tile_size = tuple(self.params.get("tile_size", [640, 640]))
        overlap = self.params.get("overlap", 0.2)
        min_visibility = self.params.get("min_visibility", 0.3)
        min_resolution = self.params.get("min_resolution_to_tile", [1280, 1280])

        files = ctx.valid_files
        if not files:
            return {"total": 0, "tiles_created": 0}

        img_dir = ctx.output_path / "images"
        if not img_dir.exists():
            img_dir = ctx.input_path

        tile_dir = ctx.output_path / "tiled_images"
        tile_dir.mkdir(parents=True, exist_ok=True)

        tracker = getattr(self, "_tracker", None)
        if tracker:
            tracker.start_stage("tile", total=len(files))

        total_tiles = 0
        tiled_images = 0

        for i, rel_path in enumerate(files):
            fpath = img_dir / rel_path
            if not fpath.exists():
                fpath = ctx.input_path / rel_path
            if not fpath.exists():
                continue

            try:
                img = cv2.imread(str(fpath))
                if img is None:
                    continue

                h, w = img.shape[:2]
                stem = Path(rel_path).stem

                # Only tile images larger than min_resolution
                if w < min_resolution[0] and h < min_resolution[1]:
                    continue

                labels = ctx.annotations.get(stem, [])
                tiles = tile_image(img, tile_size, overlap)

                for t_idx, (tile_img, tile_offset) in enumerate(tiles):
                    tile_name = f"{stem}_tile{t_idx:03d}.jpg"
                    cv2.imwrite(str(tile_dir / tile_name), tile_img)

                    tile_labels = adjust_labels_for_tile(
                        labels, tile_offset, (w, h), min_visibility
                    )
                    ctx.annotations[f"{stem}_tile{t_idx:03d}"] = tile_labels
                    total_tiles += 1

                tiled_images += 1

            except Exception as e:
                logger.warning(f"Tile error for {rel_path}: {e}")

            if tracker:
                tracker.update_stage("tile", i + 1)

        return {
            "total_images": len(files),
            "tiled_images": tiled_images,
            "tiles_created": total_tiles,
            "tile_size": list(tile_size),
            "overlap": overlap,
        }
