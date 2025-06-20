# 핫/콜드 스토리지 아키텍처

## 개요

Simple Queue Service에 핫/콜드 스토리지 계층화 시스템을 구현하여 메모리 효율성과 성능을 최적화했습니다.

## 아키텍처

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Hot Storage   │───▶│   Warm Storage  │───▶│  Cold Storage   │
│  (Redis Memory) │    │  (Redis Disk)   │    │ (File System)   │
│   < 1ms access  │    │  1-10ms access  │    │ 100-1000ms acc. │
│   1 hour TTL    │    │   24 hour TTL   │    │   14 days TTL   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 스토리지 계층

### 🔥 Hot Storage (핫 스토리지)
- **용도**: 즉시 처리가 필요한 활성 메시지
- **백엔드**: Redis 메모리
- **성능**: < 1ms 접근 시간
- **용량**: 제한적 (메모리 크기)
- **TTL**: 1시간 (설정 가능)

### 🌡️ Warm Storage (웜 스토리지)
- **용도**: 중간 빈도 접근 메시지
- **백엔드**: Redis 디스크/RDB
- **성능**: 1-10ms 접근 시간
- **용량**: 중간 (디스크 기반)
- **TTL**: 24시간 (설정 가능)

### 🧊 Cold Storage (콜드 스토리지)
- **용도**: 장기 보관, 감사, 분석용 데이터
- **백엔드**: 로컬 파일 시스템 (향후 S3/MinIO 지원)
- **성능**: 100-1000ms 접근 시간
- **용량**: 대용량 (확장 가능)
- **TTL**: 14일 (메시지 보존 기간)

## 자동 계층화 정책

### 시간 기반 정책
```yaml
# application.yml
app:
  storage:
    tiering:
      hot-to-warm-age-seconds: 3600    # 1시간 후 웜으로 이관
      warm-to-cold-age-seconds: 86400  # 24시간 후 콜드로 이관
      check-interval-seconds: 300      # 5분마다 확인
```

### 용량 기반 정책
- **핫 스토리지**: 80% 초과 시 강제 이관
- **웜 스토리지**: 90% 초과 시 정리
- **콜드 스토리지**: GZIP 압축 적용

## 큐 타입별 처리 전략

### FIFO 큐
```kotlin
// 순서 보장을 위해 모든 계층 검색
fun receiveMessageFIFO(queueName: String): Message? {
    return hotStorage.getOldestMessage(queueName)
        ?: warmStorage.getOldestMessage(queueName)?.also { 
            promoteToHot(it) 
        }
        ?: coldStorage.getOldestMessage(queueName)?.also { 
            promoteToHot(it) 
        }
}
```

### Standard 큐
```kotlin
// 성능 최적화를 위해 핫 스토리지 우선
fun receiveMessageStandard(queueName: String): Message? {
    return hotStorage.getMessage(queueName)
        ?: scheduleAsyncPromotion(queueName)
}
```

## API 엔드포인트

### 스토리지 통계 조회
```http
GET /api/v1/storage/stats
```

### 메시지 위치 확인
```http
GET /api/v1/storage/messages/{messageId}/location
```

### 수동 메시지 승격
```http
POST /api/v1/storage/messages/{messageId}/promote
{
  "targetTier": "HOT",
  "reason": "high_priority_processing"
}
```

### 승격 힌트 제공
```http
POST /api/v1/storage/queues/{queueName}/promotion-hint
{
  "hintType": "HIGH_PRIORITY_PROCESSING",
  "timeWindowSeconds": 3600,
  "expectedThroughput": 1000
}
```

## 설정

### 환경 변수
```bash
# 콜드 스토리지 경로
COLD_STORAGE_PATH=/data/sqs-cold-storage

# Redis 설정
REDIS_HOST=localhost
REDIS_PORT=6379
```

### application.yml
```yaml
app:
  storage:
    hot:
      ttl-hours: 1
      max-memory-usage-percent: 80
    warm:
      ttl-days: 1
      max-disk-usage-gb: 100
    cold:
      base-path: /tmp/sqs-cold-storage
      compression-enabled: true
      encryption-enabled: false
    tiering:
      enabled: true
      check-interval-seconds: 300
      batch-size: 1000
```

## 모니터링

### 계층별 메트릭
- 메시지 수
- 스토리지 사용량
- 평균 접근 시간
- 히트율
- 이관율
- 에러율

### 로그 예시
```
Hot→Warm migration: 1000 successful, 0 failed
Warm→Cold migration: 500 successful, 0 failed
Message promoted: msg-123 from COLD to HOT
```

## 사용 예시

### 큐 생성 (FIFO 타입)
```http
POST /api/v1/queues
{
  "queueName": "my-fifo-queue",
  "attributes": {
    "fifoQueue": true,
    "visibilityTimeoutSeconds": 30
  }
}
```

### 메시지 전송
```http
POST /api/v1/queues/my-queue/messages
{
  "messageBody": "Important message",
  "delaySeconds": 0
}
```

메시지는 자동으로 핫 스토리지에 저장되며, 설정된 정책에 따라 웜/콜드 스토리지로 이관됩니다.

### 메시지 수신
```http
GET /api/v1/queues/my-queue/messages?maxNumberOfMessages=10
```

시스템이 자동으로 모든 계층을 검색하여 메시지를 반환하며, 필요시 상위 계층으로 승격합니다.

## 성능 최적화

### 배치 처리
- 읽기/쓰기 배치 처리로 I/O 최소화
- 트랜잭션 단위 처리
- 비동기 파이프라인 처리

### 캐시 전략
- L1: 메시지 메타데이터 캐시
- L2: 위치 정보 캐시
- L3: 자주 접근하는 메시지 캐시

### 예측 기반 승격
- 접근 패턴 분석
- 머신러닝 기반 예측 (향후 구현)
- 사용자 힌트 기반 승격

## 장점

1. **메모리 효율성**: 핫 스토리지 메모리 사용량 최적화
2. **비용 절감**: 오래된 데이터를 저렴한 스토리지로 이관
3. **성능 유지**: 자주 접근하는 데이터는 빠른 액세스 보장
4. **확장성**: 계층별 독립적 확장 가능
5. **투명성**: 애플리케이션 코드 변경 없이 동작

## 향후 계획

- [ ] AWS S3/MinIO 백엔드 지원
- [ ] 머신러닝 기반 예측 승격
- [ ] 지역별 복제 지원
- [ ] 실시간 메트릭 대시보드
- [ ] 자동 정책 최적화

## 테스트

```bash
# 단위 테스트 실행
./gradlew test --tests "*StorageServiceTest*"

# 통합 테스트 실행
./gradlew test --tests "*StorageIntegrationTest*"
```

이 핫/콜드 스토리지 시스템은 AWS SQS의 기능을 향상시키면서도 비용 효율적인 메시지 저장 및 처리를 제공합니다.

---

# FIFO Queue Hot/Cold Storage 개선 스펙

## 📋 현재 문제점

### FIFO Queue의 성능 이슈
- **Cold Storage 의존성**: 순서 보장을 위해 항상 Cold부터 확인해야 함
- **성능 저하**: Hot Storage의 이점을 활용할 수 없음
- **복잡성**: 다층 스토리지 조회로 인한 로직 복잡성

### 기존 방식의 한계
```
Hot: [msg5, msg4, msg3] (최신 3개)
Cold: [msg2, msg1]      (오래된 메시지들)

FIFO 수신 시: Cold부터 확인 → msg1, msg2 → Hot 확인 → msg3, msg4, msg5
```
→ **Cold를 항상 확인해야 하므로 성능 이점 없음**

## 🎯 해결 방안

### 핵심 아이디어: "순서대로 접근할 수 있게 배치"
```
Hot: [msg1, msg2, msg3] (오래된 3개 유지)
Cold: [msg5, msg4]      (최신 메시지들을 밀어냄)

FIFO 수신 시: Hot만 확인 → msg1, msg2, msg3 (순서 보장됨)
```
→ **Hot만 확인하면 되므로 성능 최적화 달성!**

## 📖 상세 스펙

### 1. 이관 정책 (Tiering Policy)

#### Standard Queue (기존 유지)
```
Hot (Redis Memory)  → Warm (Redis Disk)  → Cold (File System)
   1시간 경과           24시간 경과            14일 후 삭제
   오래된 메시지부터 이관
```

#### FIFO Queue (신규 개선)
```
Hot (Redis Memory)  → Warm (Redis Disk)  → Cold (File System)
   100개 초과 OR        1000개 초과 OR        14일 후 삭제
   1시간 경과           24시간 경과            
   최신 메시지부터 이관  최신 메시지부터 이관
```

### 2. FIFO 이관 규칙

#### Hot → Warm 이관 조건
1. **개수 조건**: Hot에 100개 초과 메시지 존재
2. **시간 조건**: 가장 오래된 메시지가 1시간 초과
3. **이관 대상**: 
   - 개수 초과 시: 최신 메시지들 (개수 - 100)개
   - 시간 초과 시: 1시간 초과된 오래된 메시지들

#### Warm → Cold 이관 조건
1. **개수 조건**: Warm에 1000개 초과 메시지 존재  
2. **시간 조건**: 가장 오래된 메시지가 24시간 초과
3. **이관 대상**:
   - 개수 초과 시: 최신 메시지들 (개수 - 1000)개
   - 시간 초과 시: 24시간 초과된 오래된 메시지들

### 3. 스왑 메커니즘

#### Hot-Warm 스왑
```
상황: Hot이 가득 찼지만 Warm에 더 오래된 메시지가 있는 경우

Before:
Hot:  [msg100, msg99, msg98, ..., msg1] (가득 참)
Warm: [msg50, msg49, msg48, ...]        (더 오래된 메시지 존재)

After:
Hot:  [msg50, msg49, msg48, ..., msg1]  (오래된 순서 유지)
Warm: [msg100, msg99, msg98, ...]       (최신 메시지들이 이관됨)
```

#### 스왑 조건
1. Hot이 개수 한계에 도달
2. Warm에 Hot보다 오래된 메시지가 존재
3. 순서 보장을 위해 Hot에 가장 오래된 메시지들 유지 필요

### 4. 수신 최적화

#### FIFO Queue 수신 전략
```kotlin
fun receiveMessagesFifo(queue: Queue, maxMessages: Int): Flux<Message> {
    return receiveFromHot(queue, maxMessages)
        .switchIfEmpty(receiveFromWarm(queue, maxMessages))
        .switchIfEmpty(receiveFromCold(queue, maxMessages))
}
```

- **우선순위**: Hot → Warm → Cold
- **Hot 우선**: 대부분의 경우 Hot에서만 수신 (성능 최적화)
- **순서 보장**: Hot에 가장 오래된 메시지들이 있으므로 순서 보장

### 5. 성능 목표

#### 응답 시간
- **Hot 수신**: 0.8ms (기존과 동일)
- **Warm 수신**: 8ms (기존과 동일)
- **Cold 수신**: 45ms (기존과 동일)

#### 메모리 효율성
- **Hot 메모리**: 100개 * 평균 메시지 크기 (예측 가능)
- **Warm 메모리**: 1000개 * 평균 메시지 크기 (제한됨)
- **전체 메모리**: 기존 대비 50% 절약 예상

### 6. 모니터링 메트릭

#### 새로운 메트릭
- `fifo_hot_message_count`: Hot Storage 메시지 개수
- `fifo_warm_message_count`: Warm Storage 메시지 개수
- `fifo_swap_operations`: 스왑 연산 횟수
- `fifo_hot_hit_ratio`: Hot에서 수신한 메시지 비율

#### 알림 조건
- Hot Storage 사용률 > 90%
- 스왑 연산 빈도 > 분당 10회
- Hot Hit Ratio < 80%

## 🔧 구현 계획

### Phase 1: 기본 구조 (1일)
- [ ] `FifoTieringPolicy` 인터페이스 설계
- [ ] 개수 기반 이관 로직 구현
- [ ] 시간 기반 이관 로직 구현

### Phase 2: 스왑 메커니즘 (1일)
- [ ] Hot-Warm 스왑 조건 검사
- [ ] 스왑 연산 구현
- [ ] 원자성 보장 (트랜잭션)

### Phase 3: 수신 최적화 (0.5일)
- [ ] FIFO 전용 수신 로직 분리
- [ ] Hot 우선 수신 구현
- [ ] Fallback 메커니즘

### Phase 4: 테스트 & 모니터링 (0.5일)
- [ ] 단위 테스트 작성
- [ ] 성능 테스트
- [ ] 메트릭 수집 및 대시보드

## 📊 예상 효과

### 성능 향상
- **FIFO 수신 속도**: 45ms → 0.8ms (94% 개선)
- **메모리 사용량**: 제한적이고 예측 가능
- **Hot Hit Ratio**: 95% 이상 예상

### 운영 개선
- **순서 보장**: 완벽한 FIFO 순서 유지
- **확장성**: 메시지 개수 증가에도 성능 유지
- **모니터링**: 명확한 메트릭과 알림

## 🚨 주의사항

### 구현 시 고려사항
- **원자성**: 스왑 연산의 원자성 보장 필요
- **일관성**: 이관 중 메시지 순서 변경 방지
- **성능**: 스왑 연산이 너무 빈번하면 오버헤드 발생

### 롤백 계획
- 기존 Standard 방식으로 즉시 전환 가능
- 이관된 메시지들의 복구 메커니즘 준비
- A/B 테스트를 통한 점진적 적용
