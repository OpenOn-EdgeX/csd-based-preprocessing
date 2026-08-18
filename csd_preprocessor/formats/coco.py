"""COCO JSON format parser.

COCO format: single JSON file with images, annotations, categories.
Widely used for object detection (COCO 2017, etc.).
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union

logger = logging.getLogger(__name__)


class COCOParser:
    """Parse COCO JSON annotation format."""

    def __init__(self, annotation_path: Union[str, Path]):
        self.annotation_path = Path(annotation_path)
        self._data = None
        self._image_id_to_info = {}
        self._image_id_to_anns = {}
        self._cat_id_to_name = {}
        self._cat_name_to_id = {}

    def load(self):
        """Load and index the COCO annotation file."""
        logger.info(f"Loading COCO annotations from {self.annotation_path}")
        with open(self.annotation_path, "r") as f:
            self._data = json.load(f)

        # Index images by ID
        for img in self._data.get("images", []):
            self._image_id_to_info[img["id"]] = img

        # Index annotations by image ID
        for ann in self._data.get("annotations", []):
            img_id = ann["image_id"]
            if img_id not in self._image_id_to_anns:
                self._image_id_to_anns[img_id] = []
            self._image_id_to_anns[img_id].append(ann)

        # Index categories
        for cat in self._data.get("categories", []):
            self._cat_id_to_name[cat["id"]] = cat["name"]
            self._cat_name_to_id[cat["name"]] = cat["id"]

        logger.info(
            f"Loaded: {len(self._image_id_to_info)} images, "
            f"{len(self._data.get('annotations', []))} annotations, "
            f"{len(self._cat_id_to_name)} categories"
        )

    @property
    def categories(self) -> Dict[int, str]:
        """Category ID → name mapping."""
        return dict(self._cat_id_to_name)

    @property
    def category_names(self) -> List[str]:
        """Sorted list of category names."""
        return [self._cat_id_to_name[k] for k in sorted(self._cat_id_to_name.keys())]

    @property
    def image_ids(self) -> List[int]:
        return list(self._image_id_to_info.keys())

    def get_image_info(self, image_id: int) -> dict:
        return self._image_id_to_info.get(image_id, {})

    def get_image_filename(self, image_id: int) -> str:
        info = self._image_id_to_info.get(image_id, {})
        return info.get("file_name", "")

    def get_annotations(self, image_id: int) -> List[dict]:
        return self._image_id_to_anns.get(image_id, [])

    def get_bboxes_yolo(self, image_id: int, class_remap: Dict[int, int] = None,
                        transform: dict = None) -> List[dict]:
        """Get bounding boxes in YOLO format (normalized cx, cy, w, h).

        Args:
            image_id: COCO image ID
            class_remap: optional mapping from COCO cat_id → new sequential class_id
            transform: optional geometry transform applied to the image before
                labelling (resize 연산이 남긴 `resize_transform.json` 의 항목).
                `{mode, scale_x, scale_y, pad_x, pad_y, out_w, out_h}`.
                주어지지 않으면 원본 해상도 기준으로 정규화한다.

        Returns:
            list of dicts with keys: class_id, cx, cy, w, h (all normalized)
        """
        img_info = self._image_id_to_info.get(image_id)
        if not img_info:
            return []

        img_w = img_info["width"]
        img_h = img_info["height"]
        anns = self._image_id_to_anns.get(image_id, [])

        # 이미지가 letterbox/stretch 로 리사이즈된 경우 어노테이션도 같은 변환을
        # 거쳐야 한다. 그러지 않으면 라벨이 원본 기하 기준으로 남아 이미지와 어긋난다.
        if transform and transform.get("mode") in ("letterbox", "stretch"):
            sx = transform.get("scale_x", 1.0)
            sy = transform.get("scale_y", 1.0)
            pad_x = transform.get("pad_x", 0)
            pad_y = transform.get("pad_y", 0)
            canvas_w = transform.get("out_w", img_w)
            canvas_h = transform.get("out_h", img_h)
        else:
            sx = sy = 1.0
            pad_x = pad_y = 0
            canvas_w, canvas_h = img_w, img_h

        results = []
        for ann in anns:
            if ann.get("iscrowd", 0):
                continue
            bbox = ann["bbox"]  # COCO format: [x_min, y_min, width, height]
            x_min, y_min, bw, bh = bbox

            # 원본 픽셀 좌표 → 변환 후 캔버스 좌표
            x_min = x_min * sx + pad_x
            y_min = y_min * sy + pad_y
            bw = bw * sx
            bh = bh * sy

            # Convert to YOLO: normalized center x, center y, width, height
            cx = (x_min + bw / 2) / canvas_w
            cy = (y_min + bh / 2) / canvas_h
            nw = bw / canvas_w
            nh = bh / canvas_h

            # Clamp to [0, 1]
            cx = max(0.0, min(1.0, cx))
            cy = max(0.0, min(1.0, cy))
            nw = max(0.0, min(1.0, nw))
            nh = max(0.0, min(1.0, nh))

            cat_id = ann["category_id"]
            if class_remap:
                cat_id = class_remap.get(cat_id, cat_id)

            results.append({
                "class_id": cat_id,
                "cx": cx,
                "cy": cy,
                "w": nw,
                "h": nh,
            })

        return results

    def build_sequential_class_mapping(self) -> Tuple[Dict[int, int], List[str]]:
        """Build a sequential class mapping (0, 1, 2, ...) from COCO category IDs.

        COCO category IDs are not sequential (e.g., 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, ...).
        YOLO requires sequential class IDs starting from 0.

        Returns:
            (coco_id_to_yolo_id, class_names_list)
        """
        sorted_cat_ids = sorted(self._cat_id_to_name.keys())
        remap = {}
        names = []
        for new_id, old_id in enumerate(sorted_cat_ids):
            remap[old_id] = new_id
            names.append(self._cat_id_to_name[old_id])
        return remap, names

    def get_stats(self) -> Dict[str, Any]:
        """Get dataset statistics."""
        ann_counts = {}
        for img_id, anns in self._image_id_to_anns.items():
            for ann in anns:
                cat_name = self._cat_id_to_name.get(ann["category_id"], "unknown")
                ann_counts[cat_name] = ann_counts.get(cat_name, 0) + 1

        return {
            "total_images": len(self._image_id_to_info),
            "total_annotations": sum(len(v) for v in self._image_id_to_anns.values()),
            "categories": len(self._cat_id_to_name),
            "annotations_per_category": ann_counts,
            "images_with_annotations": len(self._image_id_to_anns),
            "images_without_annotations": len(self._image_id_to_info) - len(self._image_id_to_anns),
        }
