"""Filesystem watcher for detecting new raw data.

CSD runs this watcher to detect when edge devices deposit
new data into the shared storage partition (nvme2n1p3).
When new data is detected, it triggers preprocessing pipelines.
"""

import json
import logging
import time
from pathlib import Path
from typing import Callable, List, Optional, Set, Union

logger = logging.getLogger(__name__)


class DataWatcher:
    """Watch a directory for new data arrivals.

    Monitors the raw_data directory on the shared CSD partition.
    When new files appear, triggers the specified callback.

    Two detection modes:
    1. Trigger file: external system writes a trigger.json file
    2. Polling: periodic scan for new files based on mtime
    """

    def __init__(
        self,
        watch_dir: Union[str, Path],
        trigger_dir: Optional[Union[str, Path]] = None,
        poll_interval: float = 5.0,
    ):
        self.watch_dir = Path(watch_dir)
        self.trigger_dir = Path(trigger_dir) if trigger_dir else self.watch_dir / "_watcher"
        self.poll_interval = poll_interval
        self._known_files: Set[str] = set()
        self._running = False

    def scan_once(self) -> List[Path]:
        """Scan for new files since last scan.

        Returns list of newly detected files.
        """
        current_files = set()
        for f in self.watch_dir.rglob("*"):
            if f.is_file() and not f.name.startswith(".") and "_watcher" not in str(f):
                current_files.add(str(f))

        new_files = current_files - self._known_files
        self._known_files = current_files

        return [Path(f) for f in sorted(new_files)]

    def check_trigger(self) -> Optional[dict]:
        """Check for trigger file from external system.

        Edge devices or orchestrator can write a trigger.json to
        signal that new data is ready for preprocessing.

        Returns trigger data if found, None otherwise.
        """
        trigger_path = self.trigger_dir / "trigger.json"
        if trigger_path.exists():
            try:
                data = json.loads(trigger_path.read_text(encoding="utf-8"))
                # Archive the trigger
                archive_dir = self.trigger_dir / "archive"
                archive_dir.mkdir(parents=True, exist_ok=True)
                archive_name = f"trigger_{int(time.time())}.json"
                trigger_path.rename(archive_dir / archive_name)
                return data
            except Exception as e:
                logger.error(f"Error reading trigger file: {e}")
                return None
        return None

    def watch(self, callback: Callable[[List[Path]], None]):
        """Start watching for new files (blocking).

        Args:
            callback: function to call with list of new files
        """
        self._running = True
        self.trigger_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Watching {self.watch_dir} for new data (poll={self.poll_interval}s)")

        # Initial scan to populate known files
        self._known_files = set()
        for f in self.watch_dir.rglob("*"):
            if f.is_file() and not f.name.startswith("."):
                self._known_files.add(str(f))

        while self._running:
            try:
                # Check trigger file
                trigger = self.check_trigger()
                if trigger:
                    logger.info(f"Trigger detected: {trigger}")
                    new_files = self.scan_once()
                    if new_files:
                        callback(new_files)

                # Polling scan
                new_files = self.scan_once()
                if new_files:
                    logger.info(f"Detected {len(new_files)} new files")
                    callback(new_files)

            except Exception as e:
                logger.error(f"Watcher error: {e}")

            time.sleep(self.poll_interval)

    def stop(self):
        """Stop the watcher."""
        self._running = False
