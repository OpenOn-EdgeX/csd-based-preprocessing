"""Job manifest definition and parsing.

A Job defines a complete preprocessing pipeline to be executed on the CSD.
HOST creates job manifests → CSD reads and executes → results written to shared storage.
"""

import json
import uuid
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, List, Union
from datetime import datetime


@dataclass
class OperationSpec:
    """Single preprocessing operation specification."""
    op: str
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class InputSpec:
    """Input data specification."""
    data_path: str
    label_path: str = ""
    format: str = "auto"  # auto, coco, voc, yolo


@dataclass
class OutputSpec:
    """Output data specification."""
    data_path: str
    structure: str = "yolo"  # yolo, raw
    generate_data_yaml: bool = True


@dataclass
class Job:
    """Complete preprocessing job manifest.

    This is the core contract between HOST and CSD:
    - HOST writes a Job manifest JSON to the jobs/ directory
    - CSD picks it up, executes the pipeline, writes results
    - HOST reads completed results from preprocessed/ directory
    """
    job_id: str
    job_type: str  # image_preprocessing, text_preprocessing, etc.
    operations: List[OperationSpec]
    input: InputSpec
    output: OutputSpec
    created_at: str = ""
    metadata: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now().isoformat()
        if not self.job_id:
            self.job_id = f"job-{uuid.uuid4().hex[:8]}"

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False)

    def save(self, path: Union[str, Path]):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.to_json(), encoding="utf-8")

    @classmethod
    def from_dict(cls, data: dict) -> "Job":
        ops = [OperationSpec(**op) for op in data.get("operations", [])]
        inp = InputSpec(**data.get("input", {}))
        out = OutputSpec(**data.get("output", {}))
        return cls(
            job_id=data.get("job_id", ""),
            job_type=data.get("job_type", "image_preprocessing"),
            operations=ops,
            input=inp,
            output=out,
            created_at=data.get("created_at", ""),
            metadata=data.get("metadata", {}),
        )

    @classmethod
    def from_json(cls, json_str: str) -> "Job":
        return cls.from_dict(json.loads(json_str))

    @classmethod
    def load(cls, path: Union[str, Path]) -> "Job":
        path = Path(path)
        return cls.from_json(path.read_text(encoding="utf-8"))

    @classmethod
    def from_pipeline_template(
        cls,
        template_path: Union[str, Path],
        input_path: str,
        output_path: str,
        label_path: str = "",
        source_format: str = "auto",
        job_id: str = "",
    ) -> "Job":
        """Create a Job from a YAML pipeline template."""
        import yaml

        template_path = Path(template_path)
        with open(template_path, "r") as f:
            template = yaml.safe_load(f)

        pipeline = template.get("pipeline", {})
        stages = template.get("stages", []) or pipeline.get("stages", [])
        ops = [OperationSpec(op=s["op"], params=s.get("params", {})) for s in stages]

        return cls(
            job_id=job_id or f"job-{uuid.uuid4().hex[:8]}",
            job_type="image_preprocessing",
            operations=ops,
            input=InputSpec(
                data_path=input_path,
                label_path=label_path,
                format=source_format,
            ),
            output=OutputSpec(data_path=output_path),
            metadata={"template": template_path.name, "pipeline_name": pipeline.get("name", "")},
        )
