FROM python:3.11-slim

LABEL maintainer="CSD-Preprocessing"
LABEL description="keti.re.kr/csd kubelet device plugin"

RUN pip install --no-cache-dir grpcio grpcio-tools

WORKDIR /app
COPY api.proto csd_device_plugin.py ./
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. api.proto

ENTRYPOINT ["python", "-u", "csd_device_plugin.py"]
