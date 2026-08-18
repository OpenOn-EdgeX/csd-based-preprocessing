"""Pascal VOC XML format parser.

VOC format: one XML file per image with bounding box annotations.
"""

import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, List, Union

logger = logging.getLogger(__name__)


class VOCParser:
    """Parse Pascal VOC XML annotation format."""

    @staticmethod
    def parse_annotation(xml_path: Union[str, Path]) -> Dict[str, Any]:
        """Parse a single VOC XML annotation file.

        Returns:
            dict with keys: filename, width, height, objects
            objects is a list of dicts with: name, bbox (xmin, ymin, xmax, ymax)
        """
        tree = ET.parse(xml_path)
        root = tree.getroot()

        filename = root.findtext("filename", "")
        size = root.find("size")
        width = int(size.findtext("width", "0"))
        height = int(size.findtext("height", "0"))

        objects = []
        for obj in root.findall("object"):
            name = obj.findtext("name", "")
            difficult = int(obj.findtext("difficult", "0"))
            bbox_elem = obj.find("bndbox")
            bbox = {
                "xmin": float(bbox_elem.findtext("xmin", "0")),
                "ymin": float(bbox_elem.findtext("ymin", "0")),
                "xmax": float(bbox_elem.findtext("xmax", "0")),
                "ymax": float(bbox_elem.findtext("ymax", "0")),
            }
            objects.append({
                "name": name,
                "difficult": difficult,
                "bbox": bbox,
            })

        return {
            "filename": filename,
            "width": width,
            "height": height,
            "objects": objects,
        }

    @staticmethod
    def to_yolo(voc_annotation: dict, class_mapping: Dict[str, int]) -> List[dict]:
        """Convert VOC annotation to YOLO format.

        Args:
            voc_annotation: parsed VOC annotation dict
            class_mapping: class name → class_id mapping

        Returns:
            list of dicts with keys: class_id, cx, cy, w, h (normalized)
        """
        width = voc_annotation["width"]
        height = voc_annotation["height"]
        results = []

        for obj in voc_annotation["objects"]:
            name = obj["name"]
            if name not in class_mapping:
                continue

            bbox = obj["bbox"]
            bw = bbox["xmax"] - bbox["xmin"]
            bh = bbox["ymax"] - bbox["ymin"]
            cx = (bbox["xmin"] + bw / 2) / width
            cy = (bbox["ymin"] + bh / 2) / height
            nw = bw / width
            nh = bh / height

            cx = max(0.0, min(1.0, cx))
            cy = max(0.0, min(1.0, cy))
            nw = max(0.0, min(1.0, nw))
            nh = max(0.0, min(1.0, nh))

            results.append({
                "class_id": class_mapping[name],
                "cx": cx,
                "cy": cy,
                "w": nw,
                "h": nh,
            })

        return results

    @staticmethod
    def collect_classes(annotation_dir: Union[str, Path]) -> List[str]:
        """Collect all unique class names from a directory of VOC XMLs."""
        annotation_dir = Path(annotation_dir)
        classes = set()
        for xml_path in annotation_dir.glob("*.xml"):
            try:
                ann = VOCParser.parse_annotation(xml_path)
                for obj in ann["objects"]:
                    classes.add(obj["name"])
            except Exception:
                continue
        return sorted(classes)
