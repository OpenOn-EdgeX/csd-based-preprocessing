"""Statistics collection operation - dataset-wide statistics.

CSD Suitability: EXCELLENT
- Reads every image/annotation, outputs one summary JSON
- Extreme data reduction ratio: GBs of data → KBs of statistics
- Reference: Summarizer (MICRO 2022) - in-storage summarization for ML

Collects:
- Per-channel mean/std (if not already computed by normalize op)
- Class distribution (annotation count per class)
- Bounding box size distribution
- Image resolution distribution
- Dataset size summary
"""

import json
import logging
from collections import defaultdict
from pathlib import Path

import cv2
import numpy as np

from .base import BaseOperation, OperationContext
from ..core.registry import register_operation

logger = logging.getLogger(__name__)


@register_operation("statistics")
class StatisticsOperation(BaseOperation):
    csd_suitability = "EXCELLENT"
    description = "Collect comprehensive dataset statistics"

    def execute(self, ctx: OperationContext) -> dict:
        compute_mean = self.params.get("per_channel_mean", True)
        compute_std = self.params.get("per_channel_std", True)
        class_dist = self.params.get("class_distribution", True)
        bbox_stats = self.params.get("bbox_statistics", True)

        tracker = getattr(self, "_tracker", None)

        stats = {
            "dataset_summary": {},
            "class_distribution": {},
            "bbox_statistics": {},
            "resolution_distribution": {},
            "normalization": ctx.statistics.get("normalization", {}),
        }

        # Dataset summary
        files = ctx.valid_files
        stats["dataset_summary"] = {
            "total_images": len(files),
            "classes": len(ctx.class_names),
            "class_names": ctx.class_names,
        }

        # Splits summary
        if ctx.split_assignments:
            split_counts = defaultdict(int)
            for f, split in ctx.split_assignments.items():
                split_counts[split] += 1
            stats["dataset_summary"]["splits"] = dict(split_counts)

        # Class distribution from annotations
        if class_dist and ctx.annotations:
            class_counts = defaultdict(int)
            total_bboxes = 0
            images_with_ann = 0
            images_without_ann = 0

            for stem, labels in ctx.annotations.items():
                if labels:
                    images_with_ann += 1
                    for label in labels:
                        cid = label["class_id"]
                        if ctx.class_names and 0 <= cid < len(ctx.class_names):
                            class_counts[ctx.class_names[cid]] += 1
                        else:
                            class_counts[str(cid)] += 1
                        total_bboxes += 1
                else:
                    images_without_ann += 1

            stats["class_distribution"] = {
                "annotations_per_class": dict(class_counts),
                "total_bounding_boxes": total_bboxes,
                "images_with_annotations": images_with_ann,
                "images_without_annotations": images_without_ann,
                "avg_bboxes_per_image": round(total_bboxes / max(images_with_ann, 1), 2),
            }

        # Bounding box size statistics
        if bbox_stats and ctx.annotations:
            widths = []
            heights = []
            areas = []
            aspect_ratios = []

            for labels in ctx.annotations.values():
                for label in labels:
                    w, h = label["w"], label["h"]
                    widths.append(w)
                    heights.append(h)
                    areas.append(w * h)
                    if h > 0:
                        aspect_ratios.append(w / h)

            if widths:
                stats["bbox_statistics"] = {
                    "width": {
                        "mean": round(float(np.mean(widths)), 4),
                        "std": round(float(np.std(widths)), 4),
                        "min": round(float(np.min(widths)), 4),
                        "max": round(float(np.max(widths)), 4),
                    },
                    "height": {
                        "mean": round(float(np.mean(heights)), 4),
                        "std": round(float(np.std(heights)), 4),
                        "min": round(float(np.min(heights)), 4),
                        "max": round(float(np.max(heights)), 4),
                    },
                    "area": {
                        "mean": round(float(np.mean(areas)), 6),
                        "std": round(float(np.std(areas)), 6),
                        "small_objects": sum(1 for a in areas if a < 0.001),
                        "medium_objects": sum(1 for a in areas if 0.001 <= a < 0.01),
                        "large_objects": sum(1 for a in areas if a >= 0.01),
                    },
                    "aspect_ratio": {
                        "mean": round(float(np.mean(aspect_ratios)), 4) if aspect_ratios else 0,
                        "std": round(float(np.std(aspect_ratios)), 4) if aspect_ratios else 0,
                    },
                }

        # Resolution distribution (sample from output images)
        img_dir = ctx.output_path / "images"
        if not img_dir.exists():
            img_dir = ctx.input_path

        resolutions = defaultdict(int)
        sample_files = files[:min(100, len(files))]

        if tracker:
            tracker.start_stage("statistics", total=len(sample_files))

        for i, rel_path in enumerate(sample_files):
            fpath = img_dir / rel_path
            if not fpath.exists():
                fpath = ctx.input_path / rel_path
            if fpath.exists():
                try:
                    img = cv2.imread(str(fpath))
                    if img is not None:
                        h, w = img.shape[:2]
                        resolutions[f"{w}x{h}"] += 1
                except Exception:
                    pass

            if tracker:
                tracker.update_stage("statistics", i + 1)

        stats["resolution_distribution"] = dict(resolutions)

        # Save statistics
        ctx.statistics.update(stats)
        stats_path = ctx.output_path / "statistics.json"
        stats_path.write_text(
            json.dumps(stats, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        return stats
