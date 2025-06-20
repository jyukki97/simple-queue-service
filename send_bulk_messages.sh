#!/bin/bash

# test-fifo 큐에 1500개의 메시지를 전송하는 스크립트

API_URL="http://localhost:8080/api/v1/queues/test-queue/messages"
API_KEY="test-api-key"
TOTAL_MESSAGES=1500

echo "Starting to send $TOTAL_MESSAGES messages to test-queue queue..."

for i in $(seq 1 $TOTAL_MESSAGES); do
    # 메시지 생성 (올바른 DTO 형식 사용)
    MESSAGE_BODY="Test message number $i for tiering system - $(date '+%Y-%m-%d %H:%M:%S')"
    
    # JSON 페이로드 생성 (SendMessageRequest 형식에 맞춤)
    JSON_PAYLOAD=$(cat <<EOF
{
  "messageBody": "$MESSAGE_BODY",
  "delaySeconds": 0,
  "attributes": {
    "messageNumber": "$i",
    "messageGroupId": "group-$(( (i - 1) % 10 + 1 ))",
    "messageDeduplicationId": "dedup-$i-$(date +%s%N)",
    "sender": "bulk-test"
  }
}
EOF
)
    
    # API 호출
    RESPONSE=$(curl -s -X POST \
        -H "Content-Type: application/json" \
        -H "X-API-Key: $API_KEY" \
        -d "$JSON_PAYLOAD" \
        "$API_URL")
    
    # 응답 확인
    if echo "$RESPONSE" | grep -q '"success":true'; then
        if [ $((i % 100)) -eq 0 ]; then
            echo "Sent $i messages successfully"
        fi
    else
        echo "Failed to send message $i: $RESPONSE"
        # 에러가 발생해도 계속 진행
    fi
    
    # 너무 빠르게 전송하지 않도록 작은 지연 추가 (선택사항)
    if [ $((i % 50)) -eq 0 ]; then
        sleep 0.1
    fi
done

echo "Completed sending $TOTAL_MESSAGES messages!"
echo "Checking current queue status..."

# 큐 상태 확인
curl -H "X-API-Key: $API_KEY" "http://localhost:8080/api/v1/storage/queues/test-fifo/messages/by-tier" | jq '.'
