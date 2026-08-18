#!/usr/bin/env python3
"""로컬 E2E 스모크 테스트 공통 유틸."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

import numpy as np
from PIL import Image, ImageDraw

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
CLI_PATH = PROJECT_ROOT / "cli.py"
WORKER_PATH = PROJECT_ROOT / "worker" / "csd_worker.py"


def make_workspace(prefix: str) -> Path:
    return Path(tempfile.mkdtemp(prefix=prefix))


def run_cli(*args: str) -> dict:
    proc = subprocess.run(
        [sys.executable, str(CLI_PATH), *args],
        cwd=PROJECT_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise AssertionError(f"cli failed (rc={proc.returncode})\n{proc.stdout}")
    lines = proc.stdout.splitlines()
    for idx in range(len(lines) - 1, -1, -1):
        if not lines[idx].lstrip().startswith("{"):
            continue
        candidate = "\n".join(lines[idx:])
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            continue
    raise AssertionError(f"cli output is not valid JSON\n{proc.stdout}")


def run_cli_raw(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(CLI_PATH), *args],
        cwd=PROJECT_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )


def run_worker(env: dict[str, str]) -> tuple[dict, str]:
    proc = subprocess.run(
        [sys.executable, str(WORKER_PATH)],
        cwd=PROJECT_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise AssertionError(f"worker failed (rc={proc.returncode})\n{proc.stdout}")

    lines = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
    if not lines:
        raise AssertionError("worker produced no stdout")
    try:
        return json.loads(lines[-1]), proc.stdout
    except json.JSONDecodeError as exc:
        raise AssertionError(f"worker last stdout line is not JSON\n{proc.stdout}") from exc


def run_worker_raw(env: dict[str, str], timeout: float = 300) -> subprocess.CompletedProcess:
    """워커를 돌리고 성공/실패와 무관하게 결과를 그대로 돌려준다.

    run_worker() 는 실패를 예외로 바꾸므로, 실패 경로 자체를 검증할 때 쓴다."""
    return subprocess.run(
        [sys.executable, str(WORKER_PATH)],
        cwd=PROJECT_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
        timeout=timeout,
    )


def create_synthetic_coco_dataset(root: Path, n_images: int,
                                  include_annotations: bool = True) -> dict[str, Path]:
    """작은 합성 이미지 + COCO JSON 세트를 만든다."""
    raw_dir = root / "raw"
    images_dir = raw_dir / "images"
    labels_dir = raw_dir / "annotations"
    images_dir.mkdir(parents=True, exist_ok=True)
    labels_dir.mkdir(parents=True, exist_ok=True)

    images = []
    annotations = []

    for idx in range(n_images):
        file_name = f"img_{idx:03d}.jpg"
        width = 96 + (idx % 5) * 24
        height = 80 + (idx % 4) * 20
        x = 8 + (idx % 3) * 4
        y = 10 + (idx % 2) * 6
        box_w = max(20, width // 3)
        box_h = max(18, height // 3)

        yy, xx = np.mgrid[0:height, 0:width]
        arr = np.zeros((height, width, 3), dtype=np.uint8)
        arr[..., 0] = (xx * (idx + 3) + yy * 2 + idx * 17) % 256
        arr[..., 1] = (yy * (idx + 5) + xx // 2 + idx * 11) % 256
        arr[..., 2] = ((xx + yy) * (idx + 7) + idx * 23) % 256
        image = Image.fromarray(arr, mode="RGB")
        draw = ImageDraw.Draw(image)
        draw.rectangle([x, y, x + box_w, y + box_h], outline=(255, 255, 255), width=2)
        draw.rectangle([x + 3, y + 3, x + box_w - 3, y + box_h - 3], fill=(220, 30, 30))
        draw.line([0, idx % height, width - 1, (idx * 7) % height], fill=(0, 255, 0), width=2)
        image.save(images_dir / file_name, format="JPEG", quality=95)

        image_id = idx + 1
        ann_id = idx + 1
        images.append({
            "id": image_id,
            "file_name": file_name,
            "width": width,
            "height": height,
        })
        annotations.append({
            "id": ann_id,
            "image_id": image_id,
            "category_id": 1,
            "bbox": [x, y, box_w, box_h],
            "area": box_w * box_h,
            "iscrowd": 0,
        })

    annotation_file = labels_dir / "instances_test.json"
    if include_annotations:
        coco = {
            "images": images,
            "annotations": annotations,
            "categories": [{"id": 1, "name": "object", "supercategory": "synthetic"}],
        }
        annotation_file.write_text(
            json.dumps(coco, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    return {
        "raw": raw_dir,
        "images": images_dir,
        "annotations": labels_dir,
        "annotation_file": annotation_file,
    }


def count_files(path: Path, pattern: str) -> int:
    return sum(1 for _ in path.rglob(pattern))


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))
