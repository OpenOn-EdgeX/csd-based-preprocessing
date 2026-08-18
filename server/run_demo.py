#!/usr/bin/env python3
"""Run the 2-stage CSD preprocessing demo.

Simulates the real-world CSD preprocessing workflow:

  [1단계] 엣지 디바이스 → raw 데이터 도착 → CSD 즉시 전처리 (라벨 불필요)
  [사이]  HOST가 정제된 데이터를 라벨링 (CVAT, Label Studio 등)
  [2단계] 라벨링 완료 → CSD가 학습 데이터 구성 (분할, 통계, 변환)

핵심 컨셉:
  CSD가 연산 → 결과가 공유 스토리지에 기록됨 → HOST는 그냥 읽기만
"""

import json
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("demo")


def print_banner(step: int, title: str, description: str):
    logger.info("")
    logger.info("=" * 70)
    logger.info(f"  Step {step}: {title}")
    logger.info(f"  ({description})")
    logger.info("=" * 70)


def main():
    demo_data_dir = PROJECT_ROOT / "demo_data"
    raw_data_dir = demo_data_dir / "raw_data"
    stage1_output = demo_data_dir / "cleaned"         # 1단계 결과
    stage2_output = demo_data_dir / "preprocessed"     # 2단계 결과

    from csd_preprocessor.core.job import Job, OperationSpec, InputSpec, OutputSpec
    from csd_preprocessor.core.engine import PreprocessingEngine
    from csd_preprocessor.core.status import StatusTracker

    engine = PreprocessingEngine()

    # ================================================================
    # Step 1: 엣지 디바이스 → raw 데이터 도착 (시뮬레이션)
    # ================================================================
    print_banner(1,
        "엣지 디바이스에서 raw 데이터 수집",
        "엣지 카메라/센서 → nvme2n1p3 공유 파티션에 데이터 도착",
    )
    from server.download_coco_sample import setup_coco_sample
    setup_coco_sample(raw_data_dir, num_images=30)

    # ================================================================
    # Step 2: [1단계 파이프라인] CSD가 raw 데이터 즉시 정제
    #         라벨 없이 동작 가능
    # ================================================================
    print_banner(2,
        "[1단계] CSD - Raw 데이터 정제 (라벨 불필요)",
        "validate → deduplicate → filter_quality → resize → normalize",
    )

    stage1_job = Job(
        job_id="stage1-raw-ingestion",
        job_type="image_preprocessing",
        operations=[
            OperationSpec(op="validate", params={
                "check_integrity": True,
                "min_resolution": [32, 32],
                "allowed_formats": ["jpg", "jpeg", "png"],
            }),
            OperationSpec(op="deduplicate", params={
                "method": "dhash",
                "hash_size": 16,
                "threshold": 5,
            }),
            OperationSpec(op="filter_quality", params={
                "min_blur_score": 50.0,
                "min_brightness": 10,
                "max_brightness": 245,
                "min_contrast": 5.0,
            }),
            OperationSpec(op="resize", params={
                "target_size": [640, 640],
                "method": "letterbox",
                "padding_color": [114, 114, 114],
            }),
            OperationSpec(op="normalize", params={
                "per_channel_mean": True,
                "per_channel_std": True,
            }),
        ],
        input=InputSpec(
            data_path=str(raw_data_dir / "images"),
            label_path="",  # 라벨 없음!
            format="auto",
        ),
        output=OutputSpec(
            data_path=str(stage1_output),
            structure="raw",
            generate_data_yaml=False,
        ),
    )

    result1 = engine.run_job(stage1_job)
    logger.info(f"1단계 결과: {result1['status']} ({result1.get('elapsed_seconds', 0):.1f}s)")

    # 1단계 결과 확인
    if StatusTracker.is_completed(stage1_output):
        img_count = len(list((stage1_output / "images").glob("*"))) if (stage1_output / "images").exists() else 0
        logger.info(f"  정제된 이미지: {img_count}장")
        progress = StatusTracker.read_progress(stage1_output)
        if progress:
            for name, stage in progress.get("stages", {}).items():
                s = stage.get("status", "?")
                p = stage.get("processed", 0)
                t = stage.get("total", 0)
                logger.info(f"    {name:<20} [{s}] {p}/{t}")

    # ================================================================
    # Step 3: HOST가 라벨링 수행 (시뮬레이션)
    #         실제로는 CVAT, Label Studio 등에서 사람이 작업
    #         여기서는 COCO 어노테이션이 이미 있으므로 스킵
    # ================================================================
    print_banner(3,
        "[HOST] 라벨링 작업 수행",
        "HOST가 정제된 이미지를 라벨링 도구로 어노테이션 작업",
    )
    logger.info("  (데모에서는 COCO 어노테이션이 이미 존재하므로 스킵)")
    logger.info(f"  어노테이션 위치: {raw_data_dir / 'annotations'}")

    # ================================================================
    # Step 4: [2단계 파이프라인] 라벨링 완료 후 학습 데이터 구성
    # ================================================================
    print_banner(4,
        "[2단계] CSD - 학습 데이터 구성 (라벨 필요)",
        "convert_annotation → augment → split → statistics",
    )

    stage2_job = Job(
        job_id="stage2-training-prep",
        job_type="image_preprocessing",
        operations=[
            OperationSpec(op="convert_annotation", params={
                "source_format": "coco",
                "target_format": "yolo",
            }),
            OperationSpec(op="augment", params={
                "methods": ["mosaic", "cutout"],
                "num_augmented": 5,
                "target_size": [640, 640],
                "seed": 42,
            }),
            OperationSpec(op="split", params={
                "method": "stratified",
                "ratios": {"train": 0.7, "val": 0.2, "test": 0.1},
                "seed": 42,
            }),
            OperationSpec(op="statistics", params={
                "per_channel_mean": True,
                "per_channel_std": True,
                "class_distribution": True,
                "bbox_statistics": True,
            }),
        ],
        input=InputSpec(
            data_path=str(stage1_output / "images"),   # 1단계 결과를 입력으로
            label_path=str(raw_data_dir / "annotations"),  # 라벨링 결과
            format="coco",
        ),
        output=OutputSpec(
            data_path=str(stage2_output),
            structure="yolo",
            generate_data_yaml=True,
        ),
    )

    result2 = engine.run_job(stage2_job)
    logger.info(f"2단계 결과: {result2['status']} ({result2.get('elapsed_seconds', 0):.1f}s)")

    # ================================================================
    # Step 5: HOST가 최종 결과 확인 (Zero-Copy 읽기)
    # ================================================================
    print_banner(5,
        "[HOST] 전처리 완료된 학습 데이터 확인",
        "HOST는 공유 스토리지에서 완성된 데이터를 그냥 읽기만 하면 됨",
    )

    if StatusTracker.is_completed(stage2_output):
        logger.info("  Job Status: COMPLETED")
    elif StatusTracker.is_failed(stage2_output):
        logger.info("  Job Status: FAILED")

    for split in ["train", "val", "test"]:
        img_dir = stage2_output / split / "images"
        lbl_dir = stage2_output / split / "labels"
        n_img = len(list(img_dir.glob("*"))) if img_dir.exists() else 0
        n_lbl = len(list(lbl_dir.glob("*"))) if lbl_dir.exists() else 0
        logger.info(f"  {split}/: {n_img} images, {n_lbl} labels")

    data_yaml = stage2_output / "data.yaml"
    if data_yaml.exists():
        import yaml
        with open(data_yaml) as f:
            data = yaml.safe_load(f)
        logger.info(f"\n  data.yaml:")
        logger.info(f"    Classes: {data.get('nc', 0)}")
        names = data.get('names', [])
        logger.info(f"    Names: {names[:10]}{'...' if len(names) > 10 else ''}")

    stats_path = stage2_output / "statistics.json"
    if stats_path.exists():
        stats = json.loads(stats_path.read_text())
        class_dist = stats.get("class_distribution", {})
        if class_dist:
            logger.info(f"    Total bboxes: {class_dist.get('total_bounding_boxes', 0)}")
            logger.info(f"    Avg bboxes/image: {class_dist.get('avg_bboxes_per_image', 0)}")

    # ================================================================
    # 최종 요약
    # ================================================================
    logger.info("")
    logger.info("=" * 70)
    logger.info("  Demo Complete - 2-Stage CSD Preprocessing Pipeline")
    logger.info("")
    logger.info("  [1단계] 엣지 디바이스 raw 데이터 → CSD 즉시 정제")
    logger.info("          validate → dedup → quality → resize → normalize")
    logger.info("          * 라벨 없이 동작, 데이터 도착 즉시 실행")
    logger.info("")
    logger.info("  [사이]  HOST가 정제된 데이터를 라벨링")
    logger.info("")
    logger.info("  [2단계] 라벨링 완료 → CSD가 학습 데이터 구성")
    logger.info("          convert → augment → split → statistics")
    logger.info("          * 라벨 필수, HOST 요청 시 실행")
    logger.info("")
    logger.info("  HOST는 공유 스토리지(nvme2n1p3)에서 결과만 읽으면 됨!")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
