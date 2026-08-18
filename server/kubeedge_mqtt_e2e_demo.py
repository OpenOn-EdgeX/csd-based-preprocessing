#!/usr/bin/env python3
"""Self-contained KubeEdge DeviceTwin -> MQTT -> Server -> Preprocessor demo.

This demo is intentionally stdlib-only so it can run in constrained environments.
It simulates:

1. An edge device publishing telemetry through an MQTT broker.
2. DeviceTwin automatically deriving device state from telemetry.
3. A server collecting both telemetry and twin updates.
4. A preprocessing engine normalizing records and producing a summary.
"""

from __future__ import annotations

import json
import shutil
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable


DEVICE_NAME = "virtual-temp-sensor-01"
NODE_NAME = "edge-gpu-232"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEMO_ROOT = PROJECT_ROOT / "demo_data" / "kubeedge_mqtt_e2e"
RAW_DIR = DEMO_ROOT / "server_ingest" / "raw"
PROCESSED_DIR = DEMO_ROOT / "preprocessed"
LOG_PATH = DEMO_ROOT / "demo.log"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class DemoLogger:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text("", encoding="utf-8")

    def log(self, component: str, message: str) -> None:
        line = f"{utc_now()} [{component}] {message}"
        print(line)
        with self.path.open("a", encoding="utf-8") as fp:
            fp.write(line + "\n")


class LocalMqttBroker:
    def __init__(self, logger: DemoLogger):
        self.logger = logger
        self._subscriptions: list[tuple[str, Callable[[str, dict], None]]] = []

    def subscribe(self, topic: str, handler: Callable[[str, dict], None]) -> None:
        self._subscriptions.append((topic, handler))
        self.logger.log("MQTT", f"subscribe topic={topic}")

    def publish(self, topic: str, payload: dict) -> None:
        self.logger.log("MQTT", f"publish topic={topic} payload={json.dumps(payload, ensure_ascii=False)}")
        for sub_topic, handler in self._subscriptions:
            if sub_topic == topic:
                handler(topic, payload)


class DeviceTwin:
    def __init__(self, broker: LocalMqttBroker, logger: DemoLogger, device_name: str):
        self.broker = broker
        self.logger = logger
        self.device_name = device_name
        self.state = {
            "connection": "offline",
            "health": "unknown",
            "temperature": None,
            "humidity": None,
        }

    def update_from_telemetry(self, telemetry: dict) -> dict:
        temp = telemetry["temperature"]
        humidity = telemetry["humidity"]

        self.state["connection"] = "online"
        self.state["temperature"] = temp
        self.state["humidity"] = humidity

        if temp >= 30:
            self.state["health"] = "warning"
        else:
            self.state["health"] = "normal"

        twin_payload = {
            "device": self.device_name,
            "node": NODE_NAME,
            "timestamp": telemetry["timestamp"],
            "reported": dict(self.state),
        }
        self.logger.log(
            "DeviceTwin",
            f"reported connection={self.state['connection']} health={self.state['health']} "
            f"temperature={temp} humidity={humidity}",
        )
        self.broker.publish(f"kubeedge/twin/{self.device_name}/reported", twin_payload)
        return twin_payload


class ServerCollector:
    def __init__(self, broker: LocalMqttBroker, logger: DemoLogger):
        self.broker = broker
        self.logger = logger
        self.telemetry_records: list[dict] = []
        self.twin_records: list[dict] = []
        self.broker.subscribe(f"kubeedge/telemetry/{DEVICE_NAME}", self.on_telemetry)
        self.broker.subscribe(f"kubeedge/twin/{DEVICE_NAME}/reported", self.on_twin)

    def _append_jsonl(self, path: Path, payload: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as fp:
            fp.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def on_telemetry(self, topic: str, payload: dict) -> None:
        self.telemetry_records.append(payload)
        self._append_jsonl(RAW_DIR / "telemetry.jsonl", payload)
        self.logger.log(
            "Server",
            f"ingested telemetry seq={payload['sequence']} temperature={payload['temperature']} humidity={payload['humidity']}",
        )

    def on_twin(self, topic: str, payload: dict) -> None:
        self.twin_records.append(payload)
        self._append_jsonl(RAW_DIR / "twin.jsonl", payload)
        reported = payload["reported"]
        self.logger.log(
            "Server",
            f"ingested twin health={reported['health']} connection={reported['connection']} "
            f"temperature={reported['temperature']}",
        )


class SimplePreprocessingEngine:
    def __init__(self, logger: DemoLogger):
        self.logger = logger

    def run(self, telemetry_records: list[dict], twin_records: list[dict]) -> dict:
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        temps = [record["temperature"] for record in telemetry_records]
        hums = [record["humidity"] for record in telemetry_records]
        warnings = [record for record in twin_records if record["reported"]["health"] != "normal"]

        min_temp = min(temps)
        max_temp = max(temps)
        min_hum = min(hums)
        max_hum = max(hums)

        normalized_records = []
        temp_range = max_temp - min_temp or 1
        hum_range = max_hum - min_hum or 1
        for record in telemetry_records:
            normalized_records.append(
                {
                    "sequence": record["sequence"],
                    "device": record["device"],
                    "timestamp": record["timestamp"],
                    "temperature_c": record["temperature"],
                    "humidity_pct": record["humidity"],
                    "temperature_norm": round((record["temperature"] - min_temp) / temp_range, 4),
                    "humidity_norm": round((record["humidity"] - min_hum) / hum_range, 4),
                }
            )

        summary = {
            "device": DEVICE_NAME,
            "node": NODE_NAME,
            "telemetry_count": len(telemetry_records),
            "twin_update_count": len(twin_records),
            "temperature": {
                "min": min_temp,
                "max": max_temp,
                "avg": round(sum(temps) / len(temps), 2),
            },
            "humidity": {
                "min": min_hum,
                "max": max_hum,
                "avg": round(sum(hums) / len(hums), 2),
            },
            "warning_count": len(warnings),
            "final_device_state": twin_records[-1]["reported"] if twin_records else {},
            "generated_at": utc_now(),
        }

        normalized_path = PROCESSED_DIR / "normalized_records.jsonl"
        with normalized_path.open("w", encoding="utf-8") as fp:
            for record in normalized_records:
                fp.write(json.dumps(record, ensure_ascii=False) + "\n")

        summary_path = PROCESSED_DIR / "summary.json"
        summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

        self.logger.log(
            "Preprocessor",
            f"completed telemetry_count={summary['telemetry_count']} warning_count={summary['warning_count']} "
            f"avg_temp={summary['temperature']['avg']}",
        )
        self.logger.log("Preprocessor", f"wrote {normalized_path}")
        self.logger.log("Preprocessor", f"wrote {summary_path}")
        return summary


@dataclass
class EdgeTelemetryRecord:
    sequence: int
    temperature: int
    humidity: int

    def to_payload(self) -> dict:
        return {
            "device": DEVICE_NAME,
            "node": NODE_NAME,
            "sequence": self.sequence,
            "temperature": self.temperature,
            "humidity": self.humidity,
            "timestamp": utc_now(),
        }


def reset_demo_dirs() -> None:
    if DEMO_ROOT.exists():
        shutil.rmtree(DEMO_ROOT)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def run_demo() -> dict:
    reset_demo_dirs()
    logger = DemoLogger(LOG_PATH)
    broker = LocalMqttBroker(logger)
    collector = ServerCollector(broker, logger)
    twin = DeviceTwin(broker, logger, DEVICE_NAME)
    preprocessor = SimplePreprocessingEngine(logger)

    logger.log("Demo", "start KubeEdge DeviceTwin -> MQTT -> Server -> Preprocessor E2E demo")
    logger.log("Demo", f"device={DEVICE_NAME} node={NODE_NAME}")

    samples = [
        EdgeTelemetryRecord(sequence=1, temperature=25, humidity=50),
        EdgeTelemetryRecord(sequence=2, temperature=28, humidity=53),
        EdgeTelemetryRecord(sequence=3, temperature=31, humidity=57),
    ]

    for sample in samples:
        payload = sample.to_payload()
        logger.log(
            "EdgeDevice",
            f"sample seq={payload['sequence']} temperature={payload['temperature']} humidity={payload['humidity']}",
        )
        broker.publish(f"kubeedge/telemetry/{DEVICE_NAME}", payload)
        twin.update_from_telemetry(payload)
        time.sleep(0.05)

    summary = preprocessor.run(collector.telemetry_records, collector.twin_records)
    logger.log(
        "Demo",
        f"complete final_health={summary['final_device_state'].get('health')} "
        f"telemetry_count={summary['telemetry_count']}",
    )
    return summary


if __name__ == "__main__":
    result = run_demo()
    print()
    print(json.dumps(result, indent=2, ensure_ascii=False))
