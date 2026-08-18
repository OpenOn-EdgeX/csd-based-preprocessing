FROM python:3.11-slim

LABEL maintainer="CSD-Preprocessing"
LABEL description="CSD-Based AI Training Data Preprocessor"

# Install system dependencies for OpenCV + SSH offload to the real CSD (10.2.1.2)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1 \
    libglib2.0-0 \
    openssh-client \
    sshpass \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# CSD file-based interface:
#   /data   = raw input (read-only mount from shared partition)
#   /output = preprocessed output (read-write mount to shared partition)
VOLUME ["/data", "/output"]

# Default: run the CLI for preprocessing pipelines.
# For the Kubernetes mock worker path, override the command with:
#   docker run --rm \
#     -e BATCH_MANIFEST_JSON='{"batchId":"csd-test-001","worker":"CSD","indexes":[0,1,2],"itemCount":3}' \
#     -e DATA_PATH=/data \
#     -e OUTPUT_DIR=/output \
#     -e WORKER_TYPE=CSD \
#     -e BATCH_ID=csd-test-001 \
#     -v /path/to/raw:/data:ro \
#     -v /path/to/output:/output \
#     csd-preprocessor python worker/mock_worker.py
ENTRYPOINT ["python", "cli.py"]
CMD ["ops"]
