"""Base class for all preprocessing operations.

Each operation follows the CSD pattern:
- Read from input directory (raw data on shared storage)
- Process data (CSD performs computation)
- Write results to output directory (HOST reads later)
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


@dataclass
class OperationContext:
    """Shared context passed between operations in a pipeline.

    This context flows through the pipeline, allowing operations
    to communicate results (e.g., valid file list, class mapping).
    """
    input_path: Path
    output_path: Path
    label_path: Path = None
    source_format: str = "auto"

    # Shared state between operations
    valid_files: List[str] = field(default_factory=list)
    class_names: List[str] = field(default_factory=list)
    class_mapping: Dict[str, int] = field(default_factory=dict)
    annotations: Dict[str, Any] = field(default_factory=dict)
    split_assignments: Dict[str, str] = field(default_factory=dict)
    statistics: Dict[str, Any] = field(default_factory=dict)

    # Working directory for intermediate files
    work_dir: Path = None

    def __post_init__(self):
        self.input_path = Path(self.input_path)
        self.output_path = Path(self.output_path)
        if self.label_path:
            self.label_path = Path(self.label_path)
        if self.work_dir is None:
            self.work_dir = self.output_path / "_work"
            self.work_dir.mkdir(parents=True, exist_ok=True)


class BaseOperation(ABC):
    """Base class for all preprocessing operations.

    CSD Offloading Classification:
    - EXCELLENT: I/O-bound, reads lots of data, outputs little → best for CSD
    - GOOD: Mixed I/O and compute, still benefits from near-data processing
    - MODERATE: Compute-heavy but still reduces data movement
    - LOW: Compute-dominated, better on HOST GPU
    """

    operation_name: str = "base"
    csd_suitability: str = "UNKNOWN"  # EXCELLENT, GOOD, MODERATE, LOW
    description: str = ""

    def __init__(self, params: Dict[str, Any] = None):
        self.params = params or {}

    @abstractmethod
    def execute(self, ctx: OperationContext) -> dict:
        """Execute the operation.

        Args:
            ctx: Shared operation context

        Returns:
            dict with operation-specific metrics/results
        """
        pass

    def validate_params(self) -> List[str]:
        """Validate operation parameters. Return list of errors."""
        return []

    def __repr__(self):
        return f"{self.__class__.__name__}(params={self.params})"
