#!/usr/bin/env python3
"""Download a sample of COCO 2017 dataset for demo purposes.

Downloads:
- COCO 2017 validation annotations (instances_val2017.json)
- A small subset of validation images (configurable count)

This simulates edge devices depositing raw data into the shared
CSD storage partition (nvme2n1p3).
"""

import json
import logging
import os
import sys
import urllib.request
import zipfile
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

COCO_ANNOTATIONS_URL = "http://images.cocodataset.org/annotations/annotations_trainval2017.zip"
COCO_VAL_IMAGES_URL = "http://images.cocodataset.org/val2017"

DEFAULT_OUTPUT_DIR = Path(__file__).parent.parent / "demo_data" / "raw_data"
DEFAULT_NUM_IMAGES = 50


def download_file(url: str, dest: Path, desc: str = ""):
    """Download a file with progress."""
    if dest.exists():
        logger.info(f"Already exists: {dest}")
        return

    dest.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Downloading {desc or url}...")

    def progress_hook(block_num, block_size, total_size):
        downloaded = block_num * block_size
        if total_size > 0:
            pct = min(100, downloaded * 100 // total_size)
            mb = downloaded / 1024 / 1024
            total_mb = total_size / 1024 / 1024
            print(f"\r  {pct}% ({mb:.1f}/{total_mb:.1f} MB)", end="", flush=True)

    urllib.request.urlretrieve(url, str(dest), reporthook=progress_hook)
    print()
    logger.info(f"Saved to {dest}")


def setup_coco_sample(output_dir: Path, num_images: int = DEFAULT_NUM_IMAGES):
    """Download and setup COCO sample dataset."""
    output_dir.mkdir(parents=True, exist_ok=True)
    images_dir = output_dir / "images"
    images_dir.mkdir(parents=True, exist_ok=True)
    annotations_dir = output_dir / "annotations"
    annotations_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: Download annotations
    ann_zip = output_dir / "_downloads" / "annotations_trainval2017.zip"
    ann_zip.parent.mkdir(parents=True, exist_ok=True)

    ann_file = annotations_dir / "instances_val2017.json"
    if not ann_file.exists():
        download_file(COCO_ANNOTATIONS_URL, ann_zip, "COCO 2017 annotations")

        logger.info("Extracting annotations...")
        with zipfile.ZipFile(str(ann_zip), "r") as zf:
            # Extract only val2017 instances
            for name in zf.namelist():
                if "instances_val2017" in name:
                    data = zf.read(name)
                    ann_file.write_bytes(data)
                    break
        logger.info(f"Annotations saved to {ann_file}")

    # Step 2: Load annotations to get image list
    logger.info("Loading annotations...")
    with open(ann_file, "r") as f:
        coco_data = json.load(f)

    images = coco_data["images"][:num_images]
    logger.info(f"Selected {len(images)} images for download")

    # Step 3: Download images
    downloaded = 0
    for i, img_info in enumerate(images):
        filename = img_info["file_name"]
        img_path = images_dir / filename

        if img_path.exists():
            downloaded += 1
            continue

        url = f"{COCO_VAL_IMAGES_URL}/{filename}"
        try:
            urllib.request.urlretrieve(url, str(img_path))
            downloaded += 1
            if (i + 1) % 10 == 0:
                logger.info(f"  Downloaded {i + 1}/{len(images)} images")
        except Exception as e:
            logger.warning(f"  Failed to download {filename}: {e}")

    logger.info(f"Downloaded {downloaded}/{len(images)} images to {images_dir}")

    # Step 4: Create a filtered annotation file with only our images
    image_ids = {img["id"] for img in images}
    filtered_annotations = [
        ann for ann in coco_data["annotations"]
        if ann["image_id"] in image_ids
    ]

    filtered_data = {
        "images": images,
        "annotations": filtered_annotations,
        "categories": coco_data["categories"],
    }

    filtered_ann_path = annotations_dir / "instances_val2017.json"
    with open(filtered_ann_path, "w") as f:
        json.dump(filtered_data, f)
    logger.info(f"Filtered annotations: {len(filtered_annotations)} annotations for {len(images)} images")

    logger.info(f"\nCOCO sample dataset ready at: {output_dir}")
    logger.info(f"  Images: {images_dir} ({downloaded} files)")
    logger.info(f"  Annotations: {annotations_dir}")

    return output_dir


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Download COCO sample dataset")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_DIR, help="Output directory")
    parser.add_argument("--num-images", type=int, default=DEFAULT_NUM_IMAGES, help="Number of images")

    args = parser.parse_args()
    setup_coco_sample(args.output, args.num_images)


if __name__ == "__main__":
    main()
