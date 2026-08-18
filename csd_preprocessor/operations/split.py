"""Split operation - Train/Val/Test dataset splitting.

CSD Suitability: GOOD
- File operations: creates directory structure and copies/links files
- Supports: random, stratified (by class distribution)
- Writes label files alongside image files

Reference:
- Scikit-learn StratifiedShuffleSplit concept applied to object detection
"""

import json
import logging
import random
import os
import shutil
from collections import defaultdict
from pathlib import Path

from .base import BaseOperation, OperationContext
from ..core.registry import register_operation
from ..formats.yolo import YOLOWriter
from typing import Dict, List

logger = logging.getLogger(__name__)


SPLIT_ORDER = ("train", "val", "test")


def ordered_splits(ratios: dict) -> list:
    """분할 이름을 **고정 순서**(train→val→test)로 반환.

    ratios 의 dict 순서에 의존하면 안 된다: 파이프라인이 PreprocessingJob CR 을
    거치면 쿠버네티스가 map 을 키 정렬(알파벳순)해서 저장하므로 YAML 의
    {train, val, test} 가 {test, train, val} 로 바뀐다. 순서에 의존하면 같은
    설정이 실행 경로(서버 직접 실행 vs 분산 CR 경유)에 따라 다른 결과를 낸다."""
    known = [s for s in SPLIT_ORDER if s in ratios]
    return known + [s for s in ratios if s not in SPLIT_ORDER]


def allocate(n: int, ratios: dict, credits: list = None) -> list:
    """n 개를 비율대로 나눈 정수 개수 목록 (ordered_splits 순서, 최대잉여법).

    누적경계를 int() 로 자르면 그룹이 작을 때 나머지가 전부 마지막 분할로 쏠린다
    (예: 클래스 그룹 1장 → 전부 test).

    credits 를 주면 **그룹 간에 소수부를 이월**한다. 계층 분할은 클래스별로
    따로 배분하는데, 1~2장짜리 그룹이 많으면 그룹마다 독립 반올림하는 순간
    전부 train 으로 쏠려 전체 비율(8:1.5:0.5)이 깨진다. 이월하면 여러 그룹에
    걸쳐 val/test 몫이 쌓여 전체 합계가 목표 비율에 수렴한다."""
    names = ordered_splits(ratios)
    total = sum(max(0.0, float(ratios[s])) for s in names) or 1.0
    carry = credits if credits is not None else [0.0] * len(names)

    want = [carry[i] + n * max(0.0, float(ratios[s])) / total for i, s in enumerate(names)]
    counts = [max(0, int(w)) for w in want]
    left = n - sum(counts)
    if left > 0:
        # 소수부 내림차순, 동률이면 SPLIT_ORDER 우선순위 (train 먼저)
        order = sorted(range(len(names)), key=lambda i: (-(want[i] - counts[i]), i))
        for i in order[:left]:
            counts[i] += 1
    elif left < 0:                       # 이월 누적으로 초과한 경우 되돌린다
        order = sorted(range(len(names)), key=lambda i: (want[i] - counts[i], i))
        for i in order:
            while left < 0 and counts[i] > 0:
                counts[i] -= 1
                left += 1
    if credits is not None:
        for i in range(len(names)):
            credits[i] = want[i] - counts[i]
    return counts



def _place(src: Path, dst: Path) -> None:
    """분할 디렉터리에 파일을 놓는다 — 같은 파일시스템이면 하드링크.

    입력(`<out>/images`)과 출력(`<out>/train|val|test/images`)은 같은 파티션에 있으므로
    복사할 이유가 없다. 하드링크는 디렉터리 엔트리만 추가하므로 데이터 이동이 없다.
    5000장 실측에서 split 이 94.3초였고 그 대부분이 shutil.copy2 였다.

    분할 후 컨트롤러가 중간 디렉터리(`<out>/images`)를 지우는데, 하드링크는 링크 수만
    줄어들 뿐이라 train/val/test 의 파일은 그대로 남는다. 이후 이 이미지를 제자리에서
    수정하는 단계는 없으므로 링크 공유가 문제되지 않는다.
    교차 파일시스템 등으로 링크가 안 되면 복사로 폴백한다.
    """
    if dst.exists():
        dst.unlink()
    try:
        os.link(str(src), str(dst))
    except OSError:
        shutil.copy2(str(src), str(dst))


@register_operation("split")
class SplitOperation(BaseOperation):
    csd_suitability = "GOOD"
    description = "Split dataset into train/val/test sets (random or stratified)"

    def execute(self, ctx: OperationContext) -> dict:
        method = self.params.get("method", "random")
        ratios = self.params.get("ratios", {"train": 0.8, "val": 0.15, "test": 0.05})
        seed = self.params.get("seed", 42)

        random.seed(seed)

        files = ctx.valid_files
        if not files:
            return {"total": 0, "splits": {}}

        # Create split directories
        for split_name in ratios:
            (ctx.output_path / split_name / "images").mkdir(parents=True, exist_ok=True)
            (ctx.output_path / split_name / "labels").mkdir(parents=True, exist_ok=True)

        # Determine assignments
        if method == "stratified" and ctx.annotations:
            assignments = self._stratified_split(files, ctx.annotations, ratios, seed)
        else:
            assignments = self._random_split(files, ratios, seed)

        ctx.split_assignments = assignments

        tracker = getattr(self, "_tracker", None)
        if tracker:
            tracker.start_stage("split", total=len(files))

        # Image source directory
        img_dir = ctx.output_path / "images"
        if not img_dir.exists():
            img_dir = ctx.input_path

        # Also check augmented images (stored in _work/ during augment step)
        aug_dir = ctx.work_dir / "augmented"

        split_counts = defaultdict(int)

        for i, rel_path in enumerate(files):
            stem = Path(rel_path).stem
            split_name = assignments.get(rel_path, "train")
            split_counts[split_name] += 1

            # Find source image
            src_path = img_dir / rel_path
            if not src_path.exists():
                src_path = ctx.input_path / rel_path
            if not src_path.exists() and aug_dir.exists():
                src_path = aug_dir / rel_path

            if src_path.exists():
                # Copy image to split directory
                dst_img = ctx.output_path / split_name / "images" / f"{stem}.jpg"
                if src_path != dst_img:
                    _place(src_path, dst_img)

            # Write YOLO label
            labels = ctx.annotations.get(stem, [])
            label_path = ctx.output_path / split_name / "labels" / f"{stem}.txt"
            if labels:
                YOLOWriter.write_labels(labels, label_path)
            else:
                YOLOWriter.write_empty(label_path)

            if tracker:
                tracker.update_stage("split", i + 1)

        # Also handle augmented files if they exist
        if aug_dir.exists():
            aug_files = list(aug_dir.glob("*.jpg"))
            for aug_file in aug_files:
                stem = aug_file.stem
                if stem not in assignments:
                    # Augmented files go to train
                    dst_img = ctx.output_path / "train" / "images" / aug_file.name
                    _place(aug_file, dst_img)

                    labels = ctx.annotations.get(stem, [])
                    label_path = ctx.output_path / "train" / "labels" / f"{stem}.txt"
                    if labels:
                        YOLOWriter.write_labels(labels, label_path)
                    else:
                        YOLOWriter.write_empty(label_path)
                    split_counts["train"] += 1

        result = {
            "total": len(files),
            "method": method,
            "splits": dict(split_counts),
        }

        # Write split report
        report_path = ctx.work_dir / "split_report.json"
        report_path.write_text(json.dumps(result, indent=2), encoding="utf-8")

        return result

    def _random_split(
        self, files: List[str], ratios: dict, seed: int
    ) -> Dict[str, str]:
        """Random split."""
        random.seed(seed)
        shuffled = list(files)
        random.shuffle(shuffled)

        assignments = {}
        for name, count in zip(ordered_splits(ratios), allocate(len(shuffled), ratios)):
            for _ in range(count):
                assignments[shuffled[len(assignments)]] = name
        return assignments

    def _stratified_split(
        self, files: List[str], annotations: dict,
        ratios: dict, seed: int
    ) -> Dict[str, str]:
        """Stratified split based on primary class in each image."""
        random.seed(seed)

        # Determine primary class for each file
        class_groups = defaultdict(list)
        for f in files:
            stem = Path(f).stem
            labels = annotations.get(stem, [])
            if labels:
                # Primary class = most frequent class in the image
                class_counts = defaultdict(int)
                for label in labels:
                    class_counts[label["class_id"]] += 1
                primary_class = max(class_counts, key=class_counts.get)
            else:
                primary_class = -1  # no annotations
            class_groups[primary_class].append(f)

        # Split each class group proportionally
        assignments = {}
        splits = ordered_splits(ratios)

        # 그룹 간 소수부 이월 — 작은 클래스 그룹이 많아도 전체 비율을 지킨다
        credits = [0.0] * len(splits)
        for class_id in sorted(class_groups):          # 결정론적 순회
            group_files = class_groups[class_id]
            random.shuffle(group_files)
            idx = 0
            for name, count in zip(splits, allocate(len(group_files), ratios, credits)):
                for f in group_files[idx:idx + count]:
                    assignments[f] = name
                idx += count

        return assignments
