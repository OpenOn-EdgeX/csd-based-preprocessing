"""YOLO TXT format reader/writer.

YOLO format: one TXT file per image, each line: class_id cx cy w h (normalized).
"""

import logging
from pathlib import Path
from typing import List, Union

logger = logging.getLogger(__name__)


class YOLOWriter:
    """Write YOLO format annotation files."""

    @staticmethod
    def write_labels(labels: List[dict], output_path: Union[str, Path]):
        """Write YOLO labels to a TXT file.

        Args:
            labels: list of dicts with keys: class_id, cx, cy, w, h
            output_path: path to write the TXT file
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        lines = []
        for label in labels:
            line = f"{label['class_id']} {label['cx']:.6f} {label['cy']:.6f} {label['w']:.6f} {label['h']:.6f}"
            lines.append(line)

        output_path.write_text("\n".join(lines) + "\n" if lines else "", encoding="utf-8")

    @staticmethod
    def write_empty(output_path: Union[str, Path]):
        """Write an empty YOLO label file (image with no objects)."""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("", encoding="utf-8")


class YOLOReader:
    """Read YOLO format annotation files."""

    @staticmethod
    def read_labels(label_path: Union[str, Path]) -> List[dict]:
        """Read YOLO labels from a TXT file.

        Returns:
            list of dicts with keys: class_id, cx, cy, w, h
        """
        label_path = Path(label_path)
        if not label_path.exists():
            return []

        text = label_path.read_text(encoding="utf-8").strip()
        if not text:
            return []

        labels = []
        for line in text.split("\n"):
            parts = line.strip().split()
            if len(parts) >= 5:
                labels.append({
                    "class_id": int(parts[0]),
                    "cx": float(parts[1]),
                    "cy": float(parts[2]),
                    "w": float(parts[3]),
                    "h": float(parts[4]),
                })
        return labels
