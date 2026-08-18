"""Validate operation - file integrity, resolution, format checking.

CSD Suitability: EXCELLENT
- Pure I/O bound: reads file headers and metadata
- Minimal compute: just checks magic bytes and dimensions
- High data reduction: filters out corrupt/invalid files early
"""

import logging
from pathlib import Path

import cv2
import numpy as np

from .base import BaseOperation, OperationContext
from ..core.parallel import map_images
from ..core.registry import register_operation

logger = logging.getLogger(__name__)

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".tif", ".webp"}


@register_operation("validate")
class ValidateOperation(BaseOperation):
    csd_suitability = "EXCELLENT"
    description = "File integrity validation, resolution and format checking"

    def execute(self, ctx: OperationContext) -> dict:
        check_integrity = self.params.get("check_integrity", True)
        min_res = self.params.get("min_resolution", [32, 32])
        max_file_size = self.params.get("max_file_size_mb", 100) * 1024 * 1024
        allowed_fmts = self.params.get("allowed_formats", ["jpg", "jpeg", "png", "bmp"])
        allowed_exts = {f".{f.lower().lstrip('.')}" for f in allowed_fmts}

        # 검사 대상: ctx.valid_files 가 이미 채워져 있으면 그것만 본다.
        # (분산 워커는 샤드 파일 목록을 미리 넣어주므로, 여기서 입력 디렉터리를
        #  다시 스캔하면 샤드 배정이 무시되고 모든 워커가 전체를 처리해 버린다.
        #  서버 파이프라인은 valid_files 가 비어 있으므로 기존처럼 전체 스캔.)
        input_path = ctx.input_path
        if ctx.valid_files:
            all_files = [input_path / f for f in ctx.valid_files]
        else:
            all_files = []
            for ext in IMAGE_EXTENSIONS:
                all_files.extend(input_path.rglob(f"*{ext}"))
                all_files.extend(input_path.rglob(f"*{ext.upper()}"))
            all_files = sorted(set(all_files))

        tracker = getattr(self, "_tracker", None)
        if tracker:
            tracker.start_stage("validate", total=len(all_files))

        valid_files = []
        invalid_files = []
        errors = 0

        # 파일 단위 검사는 서로 독립이라 병렬로 돌리고, 취합은 순서대로 한다.
        results = map_images(
            all_files,
            lambda fp: self._validate_file(
                fp, check_integrity, min_res, max_file_size, allowed_exts),
        )
        for i, (fpath, result) in enumerate(zip(all_files, results)):
            if result["valid"]:
                # Store relative path from input_path
                valid_files.append(str(fpath.relative_to(input_path)))
            else:
                invalid_files.append({
                    "file": str(fpath.relative_to(input_path)),
                    "reason": result["reason"],
                })
                errors += 1

            if tracker:
                tracker.update_stage("validate", i + 1, errors)

        # Update context with valid files list
        ctx.valid_files = valid_files

        # Write invalid files report
        report_path = ctx.work_dir / "validation_report.json"
        import json
        report_path.write_text(json.dumps({
            "total_scanned": len(all_files),
            "valid": len(valid_files),
            "invalid": len(invalid_files),
            "invalid_files": invalid_files,
        }, indent=2, ensure_ascii=False), encoding="utf-8")

        return {
            "total_scanned": len(all_files),
            "valid": len(valid_files),
            "invalid": len(invalid_files),
        }

    def _validate_file(
        self, fpath: Path, check_integrity: bool,
        min_res: list, max_file_size: int, allowed_exts: set
    ) -> dict:
        # Check extension
        if fpath.suffix.lower() not in allowed_exts:
            return {"valid": False, "reason": f"disallowed format: {fpath.suffix}"}

        # Check file size
        try:
            size = fpath.stat().st_size
            if size == 0:
                return {"valid": False, "reason": "empty file"}
            if size > max_file_size:
                return {"valid": False, "reason": f"file too large: {size / 1024 / 1024:.1f}MB"}
        except OSError as e:
            return {"valid": False, "reason": f"cannot stat: {e}"}

        # Check integrity (try to decode)
        if check_integrity:
            try:
                img = cv2.imread(str(fpath))
                if img is None:
                    return {"valid": False, "reason": "cannot decode image"}
                h, w = img.shape[:2]
                if w < min_res[0] or h < min_res[1]:
                    return {"valid": False, "reason": f"resolution too low: {w}x{h}"}
            except Exception as e:
                return {"valid": False, "reason": f"decode error: {e}"}

        return {"valid": True, "reason": ""}
