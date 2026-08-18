"""Deduplicate operation - hash-based duplicate detection and removal.

CSD Suitability: EXCELLENT
- I/O bound: reads entire file to compute hash, outputs only hash values
- High data reduction: typically removes 10-30% of web-scraped data
- Supports: MD5 (exact), pHash/dHash (perceptual, near-duplicate)

References:
- Lee et al. (2022) "Deduplicating Training Data Makes Language Models Better"
- Summarizer (MICRO 2022) - in-storage hashing for ML data
"""

import hashlib
import logging
from collections import defaultdict
from pathlib import Path

import cv2
import numpy as np

from .base import BaseOperation, OperationContext
from ..core.parallel import map_images
from ..core.registry import register_operation

logger = logging.getLogger(__name__)


def compute_md5(filepath: Path) -> str:
    """Compute MD5 hash of a file (exact duplicate detection)."""
    h = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def compute_phash(filepath: Path, hash_size: int = 16) -> str:
    """Compute perceptual hash using DCT (near-duplicate detection)."""
    img = cv2.imread(str(filepath), cv2.IMREAD_GRAYSCALE)
    if img is None:
        return ""
    resized = cv2.resize(img, (hash_size + 1, hash_size), interpolation=cv2.INTER_AREA)
    diff = resized[:, 1:] > resized[:, :-1]
    return "".join(["1" if b else "0" for b in diff.flatten()])


def compute_dhash(filepath: Path, hash_size: int = 16) -> str:
    """Compute difference hash (fast near-duplicate detection)."""
    img = cv2.imread(str(filepath), cv2.IMREAD_GRAYSCALE)
    if img is None:
        return ""
    resized = cv2.resize(img, (hash_size + 1, hash_size), interpolation=cv2.INTER_AREA)
    diff = resized[:, 1:] > resized[:, :-1]
    return "".join(["1" if b else "0" for b in diff.flatten()])


# popcount — CSD 는 python 3.8 이라 int.bit_count() 가 없다(3.10+). bin().count() 는
# 문자열을 만들지만 세는 것은 C 레벨이라 파이썬 루프보다 훨씬 빠르다.
_BIT_COUNT = getattr(int, "bit_count", None)


def _popcount(x: int) -> int:
    return _BIT_COUNT(x) if _BIT_COUNT else bin(x).count("1")


def hamming_distance(hash1: str, hash2: str) -> int:
    """Compute Hamming distance between two binary hash strings.

    비트문자열을 정수로 바꿔 XOR + popcount 로 센다. 이전 구현은 256자를 파이썬
    제너레이터로 한 글자씩 비교해 쌍당 8.9us(x86) / 232us(CSD ARM) 였고, 중복 탐지가
    쌍 비교라 파일 수의 **제곱**으로 늘어난다 — 5000장 실측에서 deduplicate 가
    stage1 전체 시간의 65~71% 를 차지했다.

    길이가 다르면 정수 변환 시 자릿수가 어긋나므로 기존 동작(공통 길이만 비교)을
    그대로 쓴다.
    """
    if len(hash1) != len(hash2):
        return sum(c1 != c2 for c1, c2 in zip(hash1, hash2))
    return _popcount(int(hash1, 2) ^ int(hash2, 2))


@register_operation("deduplicate")
class DeduplicateOperation(BaseOperation):
    csd_suitability = "EXCELLENT"
    description = "Hash-based duplicate detection and removal"

    def execute(self, ctx: OperationContext) -> dict:
        method = self.params.get("method", "phash")
        hash_size = self.params.get("hash_size", 16)
        threshold = self.params.get("threshold", 5)

        files = ctx.valid_files
        if not files:
            return {"total": 0, "unique": 0, "duplicates": 0}

        tracker = getattr(self, "_tracker", None)
        if tracker:
            tracker.start_stage("deduplicate", total=len(files))

        # Compute hashes
        hash_func = {
            "md5": lambda f: compute_md5(f),
            "phash": lambda f: compute_phash(f, hash_size),
            "dhash": lambda f: compute_dhash(f, hash_size),
        }.get(method, lambda f: compute_phash(f, hash_size))

        def _hash_one(rel_path):
            try:
                return hash_func(ctx.input_path / rel_path), None
            except Exception as e:
                return None, e

        hashed = map_images(files, _hash_one)      # 해시 계산만 병렬, 비교는 아래에서 순차

        file_hashes = {}
        errors = 0
        for i, (rel_path, (h, err)) in enumerate(zip(files, hashed)):
            if err is not None:
                logger.warning(f"Hash error for {rel_path}: {err}")
                errors += 1
            elif h:
                file_hashes[rel_path] = h

            if tracker:
                tracker.update_stage("deduplicate", i + 1, errors)

        # Find duplicates
        if method == "md5":
            # Exact matching
            hash_groups = defaultdict(list)
            for rel_path, h in file_hashes.items():
                hash_groups[h].append(rel_path)

            unique_files = []
            duplicate_groups = []
            for h, group in hash_groups.items():
                unique_files.append(group[0])
                if len(group) > 1:
                    duplicate_groups.append(group)
        else:
            # Perceptual matching with threshold
            unique_files = []
            duplicate_groups = []
            used = set()
            # 쌍 비교는 O(n^2) 라 문자열→정수 변환을 루프 밖에서 한 번만 한다.
            hash_len = len(next(iter(file_hashes.values()), ""))
            hash_list = [(p, h, int(h, 2) if len(h) == hash_len else None)
                         for p, h in file_hashes.items()]

            for i, (path_i, hash_i, int_i) in enumerate(hash_list):
                if path_i in used:
                    continue
                group = [path_i]
                for j in range(i + 1, len(hash_list)):
                    path_j, hash_j, int_j = hash_list[j]
                    if path_j in used:
                        continue
                    if int_i is not None and int_j is not None:
                        distance = _popcount(int_i ^ int_j)
                    else:                       # 길이가 다른 해시 — 기존 경로로 폴백
                        distance = hamming_distance(hash_i, hash_j)
                    if distance <= threshold:
                        group.append(path_j)
                        used.add(path_j)

                unique_files.append(path_i)
                if len(group) > 1:
                    duplicate_groups.append(group)

        # Update context
        ctx.valid_files = unique_files

        # Write dedup report
        import json
        report_path = ctx.work_dir / "dedup_report.json"
        report_path.write_text(json.dumps({
            "method": method,
            "threshold": threshold,
            "total_files": len(files),
            "unique_files": len(unique_files),
            "duplicates_removed": len(files) - len(unique_files),
            "duplicate_groups": len(duplicate_groups),
        }, indent=2), encoding="utf-8")

        return {
            "total": len(files),
            "unique": len(unique_files),
            "duplicates": len(files) - len(unique_files),
            "duplicate_groups": len(duplicate_groups),
        }
