"""Augmentation operation - Mosaic, MixUp, CutMix, CutOut.

CSD Suitability: EXCELLENT (especially Mosaic)
- Mosaic: reads 4 images, outputs 1 → 4:1 I/O to compute ratio
- MixUp: reads 2 images, blends → 2:1 ratio
- All augmentations are embarrassingly parallel per-sample

References:
- Bochkovskiy et al. (2020) "YOLOv4" - Mosaic augmentation
- Zhang et al. (2018) "mixup: Beyond Empirical Risk Minimization"
- Yun et al. (2019) "CutMix: Regularization Strategy"
- DeVries & Taylor (2017) "Improved Regularization of CNNs with Cutout"
"""

import logging
import random
from pathlib import Path

import cv2
import numpy as np

from .base import BaseOperation, OperationContext
from ..core.parallel import map_images
from ..core.registry import register_operation
from typing import List, Tuple

logger = logging.getLogger(__name__)


def mosaic_augment(
    images: List[np.ndarray],
    labels_list: List[List[dict]],
    target_size: Tuple[int, int] = (640, 640),
    rng=None,
) -> Tuple[np.ndarray, List[dict]]:
    """Create a mosaic from 4 images (YOLOv4 style).

    Combines 4 images into a single training sample,
    improving detection of small objects and varied contexts.
    """
    rng = rng or random          # 병렬 실행 시 항목별 RNG 를 주입한다
    tw, th = target_size
    # Random center point
    cx = rng.randint(tw // 4, tw * 3 // 4)
    cy = rng.randint(th // 4, th * 3 // 4)

    mosaic = np.full((th, tw, 3), 114, dtype=np.uint8)
    combined_labels = []

    # Placement regions: top-left, top-right, bottom-left, bottom-right
    placements = [
        (0, 0, cx, cy),
        (cx, 0, tw, cy),
        (0, cy, cx, th),
        (cx, cy, tw, th),
    ]

    for idx, (x1, y1, x2, y2) in enumerate(placements):
        if idx >= len(images):
            break
        img = images[idx]
        h, w = img.shape[:2]
        rw, rh = x2 - x1, y2 - y1

        # Scale image to fit the region
        scale = min(rw / w, rh / h)
        nw, nh = int(w * scale), int(h * scale)
        resized = cv2.resize(img, (nw, nh), interpolation=cv2.INTER_AREA)

        # Place in mosaic
        ox = x1 + (rw - nw) // 2
        oy = y1 + (rh - nh) // 2
        mosaic[oy:oy + nh, ox:ox + nw] = resized

        # Adjust labels
        if idx < len(labels_list):
            for label in labels_list[idx]:
                # Transform label coordinates to mosaic space
                ncx = (label["cx"] * nw + ox) / tw
                ncy = (label["cy"] * nh + oy) / th
                nw_l = label["w"] * nw / tw
                nh_l = label["h"] * nh / th

                # Clip to mosaic bounds
                if 0 < ncx < 1 and 0 < ncy < 1 and nw_l > 0.001 and nh_l > 0.001:
                    combined_labels.append({
                        "class_id": label["class_id"],
                        "cx": ncx, "cy": ncy,
                        "w": min(nw_l, 1.0), "h": min(nh_l, 1.0),
                    })

    return mosaic, combined_labels


def mixup_augment(
    img1: np.ndarray, img2: np.ndarray,
    labels1: List[dict], labels2: List[dict],
    alpha: float = 0.5,
    np_rng=None,
) -> Tuple[np.ndarray, List[dict]]:
    """MixUp augmentation - blend two images."""
    # Resize img2 to match img1
    h, w = img1.shape[:2]
    img2_resized = cv2.resize(img2, (w, h), interpolation=cv2.INTER_AREA)

    lam = (np_rng or np.random).beta(alpha, alpha)
    mixed = (img1 * lam + img2_resized * (1 - lam)).astype(np.uint8)

    # Combine labels
    combined = list(labels1) + list(labels2)
    return mixed, combined


def cutout_augment(
    image: np.ndarray,
    num_holes: int = 1,
    hole_size_ratio: float = 0.2,
    fill_value: int = 114,
    rng=None,
) -> np.ndarray:
    """CutOut augmentation - randomly mask rectangular regions."""
    h, w = image.shape[:2]
    result = image.copy()
    hole_h = int(h * hole_size_ratio)
    hole_w = int(w * hole_size_ratio)

    rng = rng or random
    for _ in range(num_holes):
        y = rng.randint(0, h - hole_h)
        x = rng.randint(0, w - hole_w)
        result[y:y + hole_h, x:x + hole_w] = fill_value

    return result


@register_operation("augment")
class AugmentOperation(BaseOperation):
    csd_suitability = "EXCELLENT"
    description = "Data augmentation: Mosaic, MixUp, CutMix, CutOut"

    def execute(self, ctx: OperationContext) -> dict:
        methods = self.params.get("methods", ["mosaic", "cutout"])
        num_augmented = self.params.get("num_augmented", 0)  # 0 = auto (match class imbalance)
        target_size = tuple(self.params.get("target_size", [640, 640]))
        seed = self.params.get("seed", 42)
        random.seed(seed)
        np.random.seed(seed)

        files = ctx.valid_files
        if not files:
            return {"augmented": 0}

        # Determine number of augmented samples
        if num_augmented <= 0:
            # Auto: generate 20% extra samples
            num_augmented = max(1, len(files) // 5)

        # Setup directories
        img_dir = ctx.output_path / "images"
        if not img_dir.exists():
            img_dir = ctx.input_path
        aug_dir = ctx.work_dir / "augmented"
        aug_dir.mkdir(parents=True, exist_ok=True)

        tracker = getattr(self, "_tracker", None)
        if tracker:
            tracker.start_stage("augment", total=num_augmented)

        augmented_count = 0

        # 1) 추첨은 순차로 — 전역 RNG 소비 순서가 곧 재현성이다(seed 고정).
        #    각 항목에는 파생 시드를 부여해, 실행이 병렬이어도 결과가 seed 로 결정된다.
        base_seed = int(seed) if seed is not None else 0
        recipes = []
        for i in range(num_augmented):
            method = random.choice(methods)
            if method == "mosaic" and len(files) >= 4:
                recipes.append((i, "mosaic", random.sample(files, 4)))
            elif method == "mixup" and len(files) >= 2:
                recipes.append((i, "mixup", random.sample(files, 2)))
            elif method == "cutout":
                recipes.append((i, "cutout", [random.choice(files)]))
            else:
                recipes.append((i, "skip", []))

        def _resolve(f):
            fpath = img_dir / f
            return fpath if fpath.exists() else ctx.input_path / f

        # 2) 디코드·합성·기록은 이미지 단위라 병렬로 돌린다. 출력 파일명이 항목마다
        #    다르므로 안전하고, 공유 상태를 건드리지 않는다(ctx.annotations 갱신은 아래).
        def _run(recipe):
            i, method, selected = recipe
            if method == "skip":
                return None
            item_rng = random.Random(base_seed * 1000003 + i)
            try:
                if method == "mosaic":
                    images, labels_list = [], []
                    for f in selected:
                        img = cv2.imread(str(_resolve(f)))
                        if img is not None:
                            images.append(img)
                            labels_list.append(ctx.annotations.get(Path(f).stem, []))
                    if len(images) < 4:
                        return None
                    aug_img, aug_labels = mosaic_augment(
                        images[:4], labels_list[:4], target_size, rng=item_rng)
                    name = f"mosaic_{i:05d}"
                elif method == "mixup":
                    img1 = cv2.imread(str(_resolve(selected[0])))
                    img2 = cv2.imread(str(_resolve(selected[1])))
                    if img1 is None or img2 is None:
                        return None
                    aug_img, aug_labels = mixup_augment(
                        img1, img2,
                        ctx.annotations.get(Path(selected[0]).stem, []),
                        ctx.annotations.get(Path(selected[1]).stem, []),
                        np_rng=np.random.RandomState(
                            (base_seed * 1000003 + i) % (2 ** 32)))
                    name = f"mixup_{i:05d}"
                else:
                    img = cv2.imread(str(_resolve(selected[0])))
                    if img is None:
                        return None
                    aug_img = cutout_augment(img, rng=item_rng)
                    aug_labels = ctx.annotations.get(Path(selected[0]).stem, [])
                    name = f"cutout_{i:05d}"
                cv2.imwrite(str(aug_dir / f"{name}.jpg"), aug_img)
                return name, aug_labels
            except Exception as e:
                logger.warning(f"Augmentation error at {i}: {e}")
                return None

        # 3) 취합은 순차 — ctx.annotations 갱신 순서를 고정한다.
        for i, produced in enumerate(map_images(recipes, _run)):
            if produced is not None:
                name, aug_labels = produced
                ctx.annotations[name] = aug_labels
                augmented_count += 1
            if tracker:
                tracker.update_stage("augment", i + 1)

        return {
            "methods": methods,
            "requested": num_augmented,
            "augmented": augmented_count,
        }
