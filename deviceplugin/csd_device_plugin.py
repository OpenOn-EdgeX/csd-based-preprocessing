#!/usr/bin/env python3
"""CSD device plugin — keti.re.kr/csd 자원을 kubelet 에 광고한다.

GPU(nvidia.com/gpu)/NPU·LSU(prism-device-plugin) 와 같은 패턴:
서버 노드에 장착된 실 CSD(NGD Newport, 내부 ARM = CSD_HOST)를 스케줄 가능한
확장 자원으로 노출한다. 파드는 resources.limits 에 keti.re.kr/csd 를 요청하고,
실제 연산 오프로드는 워커(worker/csd_worker.py)의 SSH 채널이 담당한다.

헬스체크: CSD_HOST:CSD_PORT TCP 연결(기본 10.2.1.2:22)을 주기 확인.
불통이면 Device 를 Unhealthy 로 전환 → kubelet allocatable 0 → CSD 워커가
스케줄되지 않는다(자연스러운 장애 격리). 복구 시 자동 Healthy 복귀.

kubelet 재시작 감지: kubelet.sock inode 변경 시 gRPC 서버를 재기동해 재등록한다.

Env:
  CSD_HOST         헬스체크 대상 (기본 10.2.1.2)
  CSD_PORT         헬스체크 포트 (기본 22)
  DEVICE_COUNT     광고할 디바이스 수 (기본 1)
  RESOURCE_NAME    기본 keti.re.kr/csd
  HEALTH_INTERVAL  헬스체크 주기 초 (기본 10)
"""

import logging
import os
import socket
import threading
import time
from concurrent import futures

import grpc

import api_pb2
import api_pb2_grpc

DP_DIR = "/var/lib/kubelet/device-plugins"
KUBELET_SOCK = f"{DP_DIR}/kubelet.sock"
PLUGIN_SOCK_NAME = "keti-csd.sock"
PLUGIN_SOCK = f"{DP_DIR}/{PLUGIN_SOCK_NAME}"

RESOURCE_NAME = os.environ.get("RESOURCE_NAME", "keti.re.kr/csd")
CSD_HOST = os.environ.get("CSD_HOST", "10.2.1.2")
CSD_PORT = int(os.environ.get("CSD_PORT", "22"))
DEVICE_COUNT = int(os.environ.get("DEVICE_COUNT", "1"))
HEALTH_INTERVAL = float(os.environ.get("HEALTH_INTERVAL", "10"))

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("csd-device-plugin")


def csd_reachable() -> bool:
    try:
        with socket.create_connection((CSD_HOST, CSD_PORT), timeout=3):
            return True
    except OSError:
        return False


class CsdDevicePlugin(api_pb2_grpc.DevicePluginServicer):
    def __init__(self, healthy: bool):
        self.healthy = healthy
        self.changed = threading.Event()
        self.stopped = threading.Event()

    def _devices(self):
        health = "Healthy" if self.healthy else "Unhealthy"
        return [api_pb2.Device(ID=f"csd-{i}", health=health)
                for i in range(DEVICE_COUNT)]

    def set_health(self, healthy: bool):
        if healthy != self.healthy:
            self.healthy = healthy
            log.info(f"CSD {CSD_HOST}:{CSD_PORT} -> "
                     f"{'Healthy' if healthy else 'Unhealthy'}")
            self.changed.set()

    # ---- DevicePlugin service ---------------------------------------- #
    def GetDevicePluginOptions(self, request, context):
        return api_pb2.DevicePluginOptions(
            pre_start_required=False,
            get_preferred_allocation_available=False,
        )

    def ListAndWatch(self, request, context):
        yield api_pb2.ListAndWatchResponse(devices=self._devices())
        while not self.stopped.is_set():
            if self.changed.wait(timeout=1.0):
                self.changed.clear()
                yield api_pb2.ListAndWatchResponse(devices=self._devices())

    def GetPreferredAllocation(self, request, context):
        resp = api_pb2.PreferredAllocationResponse()
        for req in request.container_requests:
            c = resp.container_responses.add()
            c.deviceIDs.extend(list(req.available_deviceIDs)[:req.allocation_size])
        return resp

    def Allocate(self, request, context):
        resp = api_pb2.AllocateResponse()
        for req in request.container_requests:
            c = resp.container_responses.add()
            c.envs["KETI_CSD_DEVICES"] = ",".join(req.devicesIDs)
            c.envs["KETI_CSD_HOST"] = CSD_HOST
        log.info("Allocate: "
                 + "; ".join(",".join(r.devicesIDs) for r in request.container_requests))
        return resp

    def PreStartContainer(self, request, context):
        return api_pb2.PreStartContainerResponse()


def register_with_kubelet():
    with grpc.insecure_channel(f"unix://{KUBELET_SOCK}") as ch:
        stub = api_pb2_grpc.RegistrationStub(ch)
        stub.Register(api_pb2.RegisterRequest(
            version="v1beta1",
            endpoint=PLUGIN_SOCK_NAME,
            resource_name=RESOURCE_NAME,
            options=api_pb2.DevicePluginOptions(),
        ), timeout=10)


def serve_once() -> None:
    """gRPC 서버 기동 → kubelet 등록 → kubelet 재시작/소켓 소실 시까지 헬스 루프."""
    if os.path.exists(PLUGIN_SOCK):
        os.unlink(PLUGIN_SOCK)

    plugin = CsdDevicePlugin(healthy=csd_reachable())
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    api_pb2_grpc.add_DevicePluginServicer_to_server(plugin, server)
    server.add_insecure_port(f"unix://{PLUGIN_SOCK}")
    server.start()

    register_with_kubelet()
    log.info(f"registered {RESOURCE_NAME} x{DEVICE_COUNT} "
             f"(initial: {'Healthy' if plugin.healthy else 'Unhealthy'}, "
             f"probe {CSD_HOST}:{CSD_PORT} every {HEALTH_INTERVAL}s)")

    kubelet_ino = os.stat(KUBELET_SOCK).st_ino
    try:
        while True:
            time.sleep(HEALTH_INTERVAL)
            plugin.set_health(csd_reachable())
            try:
                if os.stat(KUBELET_SOCK).st_ino != kubelet_ino:
                    log.warning("kubelet.sock recreated -> re-register")
                    return
            except FileNotFoundError:
                log.warning("kubelet.sock disappeared -> wait & re-register")
                return
            if not os.path.exists(PLUGIN_SOCK):
                log.warning("plugin socket removed -> re-register")
                return
    finally:
        plugin.stopped.set()
        server.stop(grace=1)


def main():
    while True:
        try:
            serve_once()
        except Exception as exc:  # noqa: BLE001 — 데몬은 죽지 않고 재시도
            log.error(f"serve loop error: {exc}")
        time.sleep(5)


if __name__ == "__main__":
    main()
