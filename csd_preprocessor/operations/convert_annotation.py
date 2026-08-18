"""Annotation format conversion operation.

CSD Suitability: GOOD
- I/O: reads annotation files (XML/JSON), writes YOLO TXT files
- Compute: coordinate transformation (simple arithmetic)
- Supports: COCO JSON → YOLO, VOC XML → YOLO
"""

import json
import logging
from pathlib import Path

from .base import BaseOperation, OperationContext
from ..core.registry import register_operation
from ..formats.coco import COCOParser
from ..formats.voc import VOCParser
from ..formats.yolo import YOLOWriter

logger = logging.getLogger(__name__)


@register_operation("convert_annotation")
class ConvertAnnotationOperation(BaseOperation):
    csd_suitability = "GOOD"
    description = "Annotation format conversion (COCO/VOC → YOLO)"

    def execute(self, ctx: OperationContext) -> dict:
        source_format = self.params.get("source_format", ctx.source_format)
        target_format = self.params.get("target_format", "yolo")

        if source_format == "auto":
            source_format = self._detect_format(ctx)

        logger.info(f"Converting annotations: {source_format} → {target_format}")

        if source_format == "coco":
            return self._convert_coco(ctx)
        elif source_format == "voc":
            return self._convert_voc(ctx)
        elif source_format == "yolo":
            logger.info("Annotations already in YOLO format, skipping conversion")
            return {"status": "skipped", "reason": "already YOLO format"}
        else:
            raise ValueError(f"Unsupported source format: {source_format}")

    def _detect_format(self, ctx: OperationContext) -> str:
        """Auto-detect annotation format."""
        label_path = ctx.label_path or ctx.input_path

        # Check for COCO JSON
        for pattern in ["*.json", "instances_*.json", "annotations/*.json"]:
            json_files = list(label_path.glob(pattern))
            if json_files:
                # Verify it's COCO format
                try:
                    with open(json_files[0]) as f:
                        data = json.load(f)
                    if "images" in data and "annotations" in data:
                        return "coco"
                except Exception:
                    pass

        # Check for VOC XML
        xml_files = list(label_path.glob("*.xml"))
        if xml_files:
            return "voc"

        # Check for YOLO TXT
        txt_files = list(label_path.glob("*.txt"))
        if txt_files:
            return "yolo"

        return "unknown"

    def _load_resize_transform(self, ctx: OperationContext) -> dict:
        """리사이즈 기하 변환을 찾는다.

        어노테이션 좌표는 **원본 해상도** 기준인데 stage1 의 `resize` 가 이미지를
        letterbox 로 바꿔놓기 때문에, 변환 없이 정규화하면 라벨이 이미지와 어긋난다.
        stage1·stage2 는 별개 실행이라 컨텍스트가 이어지지 않으므로 `resize` 가
        출력 디렉터리에 남긴 `resize_transform.json` 을 입력 쪽에서 찾아 읽는다.

        파일이 없으면 빈 dict 를 돌려주고, 호출부는 원본 기준 정규화로 폴백한다
        (리사이즈를 아예 하지 않은 파이프라인은 그게 맞는 동작이다).
        """
        inline = getattr(ctx, "resize_transform", None)
        if inline:
            return inline

        candidates = [
            ctx.input_path / "resize_transform.json",
            ctx.input_path.parent / "resize_transform.json",
            ctx.output_path / "resize_transform.json",
        ]
        for cand in candidates:
            if not cand.exists():
                continue
            try:
                with open(cand) as f:
                    data = json.load(f)
            except Exception as e:
                logger.warning(f"Failed to read {cand}: {e}")
                continue

            images = data.get("images", {})
            if data.get("method") == "center_crop":
                logger.warning(
                    "resize method=center_crop: 잘려나간 영역이 있어 어노테이션 좌표를 "
                    "안전하게 변환할 수 없다. 라벨 변환을 건너뛴다 (원본 기준 정규화)."
                )
                return {}
            logger.info(f"Applying resize transform from {cand} ({len(images)} images)")
            return images

        logger.info("No resize_transform.json found — normalizing against original image size")
        return {}

    def _convert_coco(self, ctx: OperationContext) -> dict:
        """Convert COCO JSON annotations to YOLO format."""
        label_path = ctx.label_path or ctx.input_path

        # Find COCO annotation file
        json_files = list(label_path.glob("*.json"))
        if not json_files:
            json_files = list(label_path.rglob("instances_*.json"))
        if not json_files:
            json_files = list(label_path.rglob("*.json"))
        if not json_files:
            raise FileNotFoundError(f"No COCO JSON file found in {label_path}")

        ann_file = json_files[0]
        logger.info(f"Using COCO annotation file: {ann_file}")

        parser = COCOParser(ann_file)
        parser.load()

        # Build sequential class mapping
        class_remap, class_names = parser.build_sequential_class_mapping()

        transforms = self._load_resize_transform(ctx)
        ctx.class_names = class_names
        ctx.class_mapping = {name: idx for idx, name in enumerate(class_names)}

        tracker = getattr(self, "_tracker", None)

        # If valid_files is empty (e.g. stage 2 without prior validate),
        # auto-populate from input image directory
        if not ctx.valid_files:
            from .validate import IMAGE_EXTENSIONS
            for ext in IMAGE_EXTENSIONS:
                for f in ctx.input_path.rglob(f"*{ext}"):
                    ctx.valid_files.append(str(f.relative_to(ctx.input_path)))
            ctx.valid_files = sorted(set(ctx.valid_files))
            logger.info(f"Auto-populated valid_files from input: {len(ctx.valid_files)} files")

        # Convert annotations for valid files
        valid_stems = {Path(f).stem for f in ctx.valid_files}
        converted = 0
        errors = 0

        # Build filename → image_id mapping
        filename_to_id = {}
        for img_id in parser.image_ids:
            fname = parser.get_image_filename(img_id)
            filename_to_id[Path(fname).stem] = img_id

        total = len(valid_stems)
        if tracker:
            tracker.start_stage("convert_annotation", total=total)

        # Store converted annotations in context
        for i, stem in enumerate(valid_stems):
            img_id = filename_to_id.get(stem)
            if img_id is not None:
                yolo_labels = parser.get_bboxes_yolo(
                    img_id, class_remap, transform=transforms.get(stem))
                ctx.annotations[stem] = yolo_labels
                converted += 1
            else:
                ctx.annotations[stem] = []

            if tracker:
                tracker.update_stage("convert_annotation", i + 1, errors)

        return {
            "source_format": "coco",
            "converted": converted,
            "classes": len(class_names),
            "class_names": class_names,
            "resize_transform_applied": sum(1 for s in valid_stems if s in transforms),
        }

    def _convert_voc(self, ctx: OperationContext) -> dict:
        """Convert VOC XML annotations to YOLO format."""
        label_path = ctx.label_path or ctx.input_path

        # Collect all classes first
        class_names = VOCParser.collect_classes(label_path)
        class_mapping = {name: idx for idx, name in enumerate(class_names)}
        ctx.class_names = class_names
        ctx.class_mapping = class_mapping

        tracker = getattr(self, "_tracker", None)
        valid_stems = {Path(f).stem for f in ctx.valid_files}

        converted = 0
        errors = 0
        total = len(valid_stems)

        if tracker:
            tracker.start_stage("convert_annotation", total=total)

        for i, stem in enumerate(valid_stems):
            xml_path = label_path / f"{stem}.xml"
            if xml_path.exists():
                try:
                    voc_ann = VOCParser.parse_annotation(xml_path)
                    yolo_labels = VOCParser.to_yolo(voc_ann, class_mapping)
                    ctx.annotations[stem] = yolo_labels
                    converted += 1
                except Exception as e:
                    logger.warning(f"VOC parse error for {stem}: {e}")
                    ctx.annotations[stem] = []
                    errors += 1
            else:
                ctx.annotations[stem] = []

            if tracker:
                tracker.update_stage("convert_annotation", i + 1, errors)

        return {
            "source_format": "voc",
            "converted": converted,
            "classes": len(class_names),
            "class_names": class_names,
        }
