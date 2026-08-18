#!/usr/bin/env python3
"""Minimal local test for the legacy Stage 2 trigger flow.

csd_watcher.py 의 --legacy-inprocess 제어 흐름(트리거 큐잉 → Stage 1 완료 후
Stage 2 실행)을 전처리 없이 검증한다. cv2/OpenCV 가 없어도 동작한다.
기본 watch 모드는 Stage 1/2 를 실행하지 않으므로 legacy=True 로 생성한다.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from typing import Iterable

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import server.csd_watcher as csd_watcher_module
from server.csd_watcher import CSDDataWatcher


class FlowTestServer(CSDDataWatcher):
    def __init__(self, base_dir: Path, scans: Iterable[list[str]], triggers: Iterable[dict | None]):
        super().__init__(base_dir, legacy=True)
        self._scan_sequence = iter(scans)
        self._trigger_sequence = iter(triggers)
        self.events: list[tuple[str, str]] = []

    def _scan_images(self) -> list[str]:
        try:
            new_files = next(self._scan_sequence)
        except StopIteration:
            self._running = False
            return []

        for name in new_files:
            self._known_files.add(name)
        return new_files

    def _check_trigger(self):
        try:
            return next(self._trigger_sequence)
        except StopIteration:
            return None

    def _run_stage1(self):
        self.events.append(("stage1", f"known={len(self._known_files)}"))
        self._stage1_done = True

    def _run_stage2(self, label_path: str = ""):
        self.events.append(("stage2", label_path or "<default>"))
        self._running = False


def assert_events(name: str, actual: list[tuple[str, str]], expected: list[tuple[str, str]]):
    if actual != expected:
        raise AssertionError(f"{name} failed\nexpected={expected}\nactual={actual}")
    print(f"[PASS] {name}: {actual}")


def make_base_dir() -> Path:
    temp_dir = Path(tempfile.mkdtemp(prefix="csd-trigger-flow-"))
    (temp_dir / "raw_data" / "images").mkdir(parents=True, exist_ok=True)
    (temp_dir / "raw_data" / "_watcher").mkdir(parents=True, exist_ok=True)
    return temp_dir


def run_scenario_trigger_before_stage1():
    base_dir = make_base_dir()
    label_path = str(base_dir / "raw_data" / "annotations")
    server = FlowTestServer(
        base_dir=base_dir,
        scans=[
            [],              # initial scan
            ["a.jpg"],       # first loop detects new images
            [],              # debounce follow-up scan
        ],
        triggers=[
            {"stage": "stage2", "label_path": label_path},
        ],
    )
    server.run()
    assert_events(
        "trigger queued before stage1 completion",
        server.events,
        [("stage1", "known=1"), ("stage2", label_path)],
    )


def run_scenario_trigger_after_stage1():
    base_dir = make_base_dir()
    label_path = str(base_dir / "raw_data" / "annotations")
    server = FlowTestServer(
        base_dir=base_dir,
        scans=[
            [],              # initial scan
            ["a.jpg"],       # first loop detects new images
            [],              # debounce follow-up scan
            [],              # second loop
        ],
        triggers=[
            None,            # first loop
            {"stage": "stage2", "label_path": label_path},
        ],
    )
    server.run()
    assert_events(
        "trigger processed after stage1 completion",
        server.events,
        [("stage1", "known=1"), ("stage2", label_path)],
    )


def main():
    original_sleep = csd_watcher_module.time.sleep
    csd_watcher_module.time.sleep = lambda _seconds: None
    try:
        run_scenario_trigger_before_stage1()
        run_scenario_trigger_after_stage1()
    finally:
        csd_watcher_module.time.sleep = original_sleep


if __name__ == "__main__":
    main()
