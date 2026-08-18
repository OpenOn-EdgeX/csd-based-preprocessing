#!/bin/bash
##############################################################################
# Edge Device -> CSD 데이터 전송 시뮬레이션 (via SCP)
#
# 엣지 디바이스(카메라, 센서)가 수집한 raw 이미지를
# SCP를 통해 CSD(10.2.1.2)의 스토리지로 전송하는 것을 시뮬레이션.
#
# 사용법:
#   ./send_data.sh          (기본: 10장씩 배치 전송)
#   ./send_data.sh --all    (전체 한번에 전송)
##############################################################################

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$SCRIPT_DIR/demo_data/_source/images"

# CSD connection
CSD_HOST="root@10.2.1.2"
CSD_BASE="/home/ngd/storage/csd_preprocessing"
CSD_TARGET="$CSD_BASE/raw_data/images"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# CSD 인증 — sshpass 비밀번호 인증 전용. 공개키는 쓰지 않는다(PubkeyAuthentication=no):
# 실행하는 자리마다 키가 있기도 없기도 해서, 켜 두면 환경에 따라 다르게 동작한다.
CSD_PASS="${CSD_PASS:?CSD_PASS 환경변수를 설정하세요}"
if ! command -v sshpass &>/dev/null; then
    echo "Error: sshpass not installed"
    exit 1
fi
SSH_OPTS=(-o StrictHostKeyChecking=no -o PubkeyAuthentication=no
          -o PreferredAuthentications=password)
SSH_CMD=(sshpass -p "$CSD_PASS" ssh "${SSH_OPTS[@]}")
SCP_CMD=(sshpass -p "$CSD_PASS" scp "${SSH_OPTS[@]}")

# Check source exists
if [ ! -d "$SOURCE_DIR" ]; then
    echo "Error: Source data not found at $SOURCE_DIR"
    echo "Run './run_local_python.sh server/download_coco_sample.py' first"
    exit 1
fi

# Clean target on CSD
if ! "${SSH_CMD[@]}" "$CSD_HOST" "rm -rf '$CSD_TARGET' && mkdir -p '$CSD_TARGET'"; then
    echo "Error: failed to prepare target directory on CSD: $CSD_TARGET"
    exit 1
fi

echo ""
echo -e "${GREEN}============================================${NC}"
echo -e "${GREEN}  Edge Device -> CSD Data Transfer (SCP)${NC}"
echo -e "${GREEN}============================================${NC}"
echo ""

FILES=($(ls "$SOURCE_DIR"/*.jpg 2>/dev/null | sort))
TOTAL=${#FILES[@]}

if [ "$TOTAL" -eq 0 ]; then
    echo "No images found in $SOURCE_DIR"
    exit 1
fi

if [ "$1" == "--all" ]; then
    echo -e "${YELLOW}[Edge]${NC} Sending $TOTAL images at once..."
    "${SCP_CMD[@]}" "$SOURCE_DIR"/*.jpg "$CSD_HOST:$CSD_TARGET/"
    echo -e "${YELLOW}[Edge]${NC} Done! $TOTAL images transferred to CSD"
else
    BATCH_SIZE=10
    SENT=0

    while [ $SENT -lt $TOTAL ]; do
        END=$((SENT + BATCH_SIZE))
        if [ $END -gt $TOTAL ]; then
            END=$TOTAL
        fi

        BATCH_COUNT=$((END - SENT))
        echo -e "${YELLOW}[Edge]${NC} Sending batch: ${BATCH_COUNT} images (${END}/${TOTAL})..."

        BATCH_FILES=""
        for ((i=SENT; i<END; i++)); do
            BATCH_FILES="$BATCH_FILES ${FILES[$i]}"
        done
        "${SCP_CMD[@]}" $BATCH_FILES "$CSD_HOST:$CSD_TARGET/"

        SENT=$END

        if [ $SENT -lt $TOTAL ]; then
            echo -e "${YELLOW}[Edge]${NC} Waiting 1s before next batch..."
            sleep 1
        fi
    done

    echo ""
    echo -e "${GREEN}[Edge] Transfer complete: ${TOTAL} images -> CSD${NC}"
fi

echo ""
echo "  CSD server will auto-detect and start Stage 1 preprocessing."
echo ""
