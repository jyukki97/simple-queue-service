# Simple Queue Service
AWS SQS와 유사한 경량 메시지 큐 시스템 구현

## 프로젝트 개요
분산 애플리케이션 간 비동기 메시지 통신을 위한 경량 큐 서비스입니다. AWS SQS의 핵심 기능들을 참고하여 구현하되, 사이드 프로젝트에 적합한 수준으로 단순화했습니다.

## 특징
- **멀티 테넌트 지원**: 고객 단위로 완전히 격리된 큐 시스템
- **지연 큐 (Delay Queue)**: 메시지 전송 후 일정 시간 지연 후 수신 가능
- **가시성 타임아웃 (Visibility Timeout)**: 메시지 처리 중 다른 컨슈머의 중복 처리 방지
- **데드레터 큐 (DLQ)**: 처리 실패한 메시지의 자동 이동
- **메시지 보존**: 최대 14일간 메시지 보관
- **REST API**: 표준 HTTP API를 통한 모든 작업 수행
- **모니터링**: 큐별 메트릭 및 상태 조회

## 주요 도메인

### Tenant (테넌트)
- 고객을 나타내는 최상위 격리 단위
- 각 테넌트는 독립된 큐 네임스페이스를 가짐
- API Key 기반 인증

### Queue (큐)
- 메시지를 저장하는 컨테이너
- 테넌트별로 관리되며, 큐 이름은 테넌트 내에서 유니크
- FIFO 보장 (순서 유지)

### Message (메시지)
- 큐를 통해 전달되는 데이터 단위
- 최대 256KB 크기 제한
- 메타데이터와 함께 저장

## 주요 기능

### 큐 관리
- [x] 큐 생성 (CreateQueue)
  - 큐 속성 설정 (VisibilityTimeout, MessageRetentionPeriod, DelaySeconds 등)
  - DLQ 설정
- [x] 큐 삭제 (DeleteQueue)
  - 큐와 모든 메시지 완전 삭제
- [x] 큐 목록 조회 (ListQueues)
  - 테넌트별 큐 목록
  - 페이징 지원 (limit, offset)
  - 큐 이름 패턴 필터링
- [x] 큐 속성 조회 (GetQueueAttributes)
- [x] 큐 속성 수정 (SetQueueAttributes)

### 메시지 관리
- [x] 메시지 전송 (SendMessage)
  - 단일 메시지 전송
  - DelaySeconds 지원
  - 메시지 속성 (attributes) 지원
- [x] 메시지 일괄 전송 (SendMessageBatch)
  - 최대 10개 메시지 동시 전송
- [x] 메시지 수신 (ReceiveMessage)
  - Long Polling 지원
  - MaxNumberOfMessages (1-10개)
  - VisibilityTimeout 적용
- [x] 메시지 삭제 (DeleteMessage)
  - 처리 완료된 메시지 삭제
- [x] 메시지 일괄 삭제 (DeleteMessageBatch)

### 모니터링 및 운영
- [x] 큐 퍼지 (PurgeQueue) - 모든 메시지 삭제
- [x] 큐 통계 조회 (GetQueueStats)
  - 대기 중인 메시지 수 (ApproximateNumberOfMessages)
  - 처리 중인 메시지 수 (ApproximateNumberOfMessagesNotVisible)
  - DLQ 메시지 수 (ApproximateNumberOfMessagesInDLQ)
  - 가장 오래된 메시지 나이 (ApproximateAgeOfOldestMessage)
- [x] 메시지 내용 미리보기 (PeekMessage)
  - 큐에서 메시지를 제거하지 않고 내용만 조회
  - 최대 10개 메시지 미리보기
  - 메시지 순서 보장

### 고급 기능 (Advanced Features)
- [] 메시지 필터링 (Message Filtering)
  - 메시지 속성 기반 필터링
  - SQL-like 필터 문법 지원
  - 구독자별 독립적인 필터 설정
- [x] 스케줄링 작업 (Scheduled Tasks)
  - 자동 DLQ 이동 (Dead Letter Queue Transfer)
  - 만료 메시지 정리 (Expired Message Cleanup)
  - 가시성 타임아웃 복원 (Visibility Timeout Recovery)
- [x] 실시간 메트릭 (Real-time Metrics)
  - 시간별/일별 메시지 처리량 통계
  - 에러율 및 지연 시간 모니터링
  - Prometheus 메트릭 내보내기
  - 큐별/시스템 전체 메트릭 수집
  - 실시간 알림 시스템
  - 커스텀 알림 규칙 설정
- [] 알림 시스템 (Notification System)
  - 큐 크기 임계값 알림
  - DLQ 메시지 누적 알림
  - 시스템 에러 알림
  - Webhook 지원

### 보안 및 인증 강화
- [] JWT 기반 인증
  - 시간 제한 토큰
  - 역할 기반 접근 제어 (RBAC)
  - 토큰 갱신 메커니즘
- [] 메시지 암호화
  - AES-256 메시지 내용 암호화
  - 테넌트별 암호화 키 관리
  - 투명한 암복호화

### 성능 최적화
- [] 큐 파티셔닝 (Queue Partitioning)
  - 높은 처리량을 위한 큐 분할
  - 라운드 로빈 메시지 분산
  - 파티션별 독립적인 처리
- [] 메시지 압축 (Message Compression)
  - GZIP 압축으로 저장 공간 절약
  - 자동 압축/해제
  - 압축률 통계 제공
- [] 캐시 최적화
  - 큐 메타데이터 캐싱
  - 빈번한 조회 데이터 캐싱
  - Redis 메모리 사용량 최적화

### 관리자 기능
- [] 관리자 대시보드
  - 전체 시스템 상태 모니터링
  - 테넌트별 사용량 통계
  - 실시간 큐 상태 시각화
- [] 테넌트 관리
  - 테넌트 생성/수정/삭제
  - 사용량 제한 설정 (Rate Limiting)
  - 저장 공간 할당량 관리

### 고가용성 및 확장성 (High Availability & Scalability)
- [] Redis 클러스터 지원
  - Redis Cluster 모드 지원
  - 샤딩 및 자동 장애 조치
  - 읽기 전용 복제본 활용
- [] 로드 밸런싱
  - 다중 인스턴스 배포
  - 세션 무상태 설계
  - 헬스 체크 엔드포인트
- [] 백업 및 복구 시스템
  - 자동 백업 스케줄링
  - 증분 백업 지원
  - 재해 복구 시나리오

### 메시지 추적 및 감사 (Message Tracing & Auditing)
- [] 메시지 추적 시스템
  - 메시지 생명 주기 추적
  - 분산 추적 (Distributed Tracing)
  - OpenTelemetry 통합
- [] 감사 로그
  - 모든 API 호출 기록
  - 메시지 처리 이력
  - 보안 이벤트 로깅
- [] 메시지 복제 및 포워딩
  - 다중 환경 메시지 복제
  - 외부 시스템 포워딩
  - 메시지 변환 파이프라인

### 고급 큐 기능 (Advanced Queue Features)
- [] FIFO 큐 강화
  - 메시지 중복 제거 (Deduplication)
  - 정확히 한 번 처리 보장
  - 메시지 그룹핑 지원
- [] 우선순위 큐
  - 메시지 우선순위 기반 처리
  - 다단계 우선순위 지원
  - 기아 방지 메커니즘
- [] 트랜잭션 지원
  - 원자적 메시지 처리
  - 롤백 지원
  - 분산 트랜잭션 (2PC)

### DevOps 및 모니터링 강화
- [] 메트릭 대시보드
  - Grafana 대시보드 템플릿
  - 커스텀 메트릭 수집
  - SLA 모니터링
- [] 로그 집계 및 분석
  - ELK Stack 통합
  - 구조화된 로깅
  - 로그 분석 대시보드
- [] 자동 스케일링
  - 큐 길이 기반 스케일링
  - CPU/메모리 기반 스케일링
  - Kubernetes HPA 통합

### API 및 통합 확장
- [] GraphQL API
  - 유연한 데이터 조회
  - 실시간 구독 지원
  - GraphQL Playground
- [] WebSocket 실시간 알림
  - 큐 상태 변경 알림
  - 메시지 도착 알림
  - 실시간 메트릭 스트리밍
- [] gRPC API
  - 고성능 바이너리 프로토콜
  - 스트리밍 지원
  - Protocol Buffers 스키마

### 고급 보안 기능
- [] API Gateway 통합
  - 중앙화된 인증/인가
  - Rate Limiting
  - API 키 관리
- [] 메시지 서명 및 검증
  - 디지털 서명 지원
  - 메시지 무결성 검증
  - 비대칭 키 암호화
- [] 네트워크 보안
  - TLS 1.3 지원
  - 상호 TLS 인증
  - IP 화이트리스트

### 성능 분석 및 최적화
- [] 성능 프로파일링
  - APM 도구 통합
  - 메모리 사용량 분석
  - CPU 성능 분석
- [] 벤치마킹 도구
  - 부하 테스트 도구
  - 성능 회귀 테스트
  - 스트레스 테스트
- [] 자동 튜닝
  - Redis 설정 최적화
  - JVM 파라미터 튜닝
  - 연결 풀 최적화

### 데이터 관리 고도화
- [] 데이터 아카이빙
  - 오래된 메시지 아카이브
  - 콜드 스토리지 이관
  - 데이터 생명 주기 관리
- [] 데이터 분석
  - 메시지 패턴 분석
  - 사용량 트렌드 분석
  - 예측 분석 기능
- [] 데이터 내보내기/가져오기
  - CSV/JSON 내보내기
  - 대량 데이터 가져오기
  - 스키마 마이그레이션

### 지능형 메시징 (Intelligent Messaging)
- [] AI 기반 메시지 분류 및 라우팅
  - 메시지 내용 기반 자동 분류
  - 머신러닝 모델을 통한 큐 추천
  - 자연어 처리 기반 메시지 분석
- [] 자동 큐 최적화
  - 사용 패턴 학습을 통한 큐 설정 자동 조정
  - 처리량 예측 및 사전 스케일링
  - 이상 패턴 탐지 및 자동 대응
- [] 스마트 알림 시스템
  - 컨텍스트 인식 알림
  - 중요도 기반 알림 우선순위
  - 알림 피로도 방지 시스템

### 엔터프라이즈 통합 (Enterprise Integration)
- [] 외부 시스템 연동
  - Apache Kafka 브리지
  - RabbitMQ 호환 모드
  - AWS SQS/SNS 마이그레이션 도구
- [] ERP/CRM 통합
  - SAP 시스템 연동
  - Salesforce 통합
  - 표준 EDI 메시지 지원
- [] 마이크로서비스 오케스트레이션
  - Saga 패턴 지원
  - 분산 워크플로우 관리
  - 서비스 메시 통합

### 규정 준수 및 거버넌스 (Compliance & Governance)
- [] 데이터 거버넌스
  - 개인정보 자동 탐지 및 마스킹
  - GDPR/CCPA 준수 기능
  - 데이터 보존 정책 자동화
- [] 컴플라이언스 모니터링
  - SOX 법안 준수 감사 로그
  - PCI DSS 보안 요구사항 충족
  - ISO 27001 보안 표준 준수
- [] 정책 기반 관리
  - 세밀한 권한 제어 (ABAC)
  - 데이터 분류 및 레이블링
  - 자동 정책 시행

### 고급 분석 및 BI (Advanced Analytics & BI)
- [] 실시간 스트림 분석
  - Apache Flink 통합
  - 복잡 이벤트 처리 (CEP)
  - 실시간 이상 탐지
- [] 비즈니스 인텔리전스
  - 메시지 패턴 기반 비즈니스 인사이트
  - 예측 분석 대시보드
  - ROI 분석 도구
- [] 머신러닝 통합
  - AutoML 기반 패턴 분석
  - 이상 탐지 모델 자동 학습
  - 예측 모델링 플랫폼

### 차세대 기술 통합 (Next-Gen Technology Integration)
- [] 블록체인 통합
  - 메시지 무결성을 위한 블록체인 해싱
  - 스마트 컨트랙트 기반 SLA 관리
  - 분산 원장 기반 감사 추적
- [] IoT 및 엣지 컴퓨팅
  - MQTT 브리지
  - 엣지 디바이스용 경량 클라이언트
  - 실시간 센서 데이터 처리
- [] 양자 암호화 준비
  - 포스트 양자 암호화 알고리즘 지원
  - 양자 키 분배 (QKD) 준비
  - 미래 지향적 보안 아키텍처

### 클라우드 네이티브 고도화 (Cloud-Native Advanced Features)
- [] 멀티 클라우드 지원
  - AWS/Azure/GCP 동시 배포
  - 클라우드 간 데이터 동기화
  - 클라우드 비용 최적화 도구
- [] 서버리스 통합
  - AWS Lambda/Azure Functions 트리거
  - 이벤트 기반 서버리스 아키텍처
  - 콜드 스타트 최적화
- [] 컨테이너 오케스트레이션 고도화
  - Istio 서비스 메시 통합
  - Knative 이벤트 처리
  - ArgoCD GitOps 파이프라인

### 개발자 경험 향상 (Developer Experience Enhancement)
- [] 통합 개발 환경
  - VS Code 확장 프로그램
  - IntelliJ IDEA 플러그인
  - 실시간 큐 디버깅 도구
- [] 코드 생성 도구
  - 클라이언트 SDK 자동 생성
  - API 문서 자동 생성
  - 테스트 케이스 자동 생성
- [] 개발자 포털
  - 인터랙티브 API 탐색기
  - 샘플 코드 갤러리
  - 커뮤니티 포럼 및 지원

### 비즈니스 연속성 (Business Continuity)
- [] 재해 복구 자동화
  - RTO/RPO 목표 자동 달성
  - 지리적 분산 백업
  - 자동 페일오버 시스템
- [] 무중단 업그레이드
  - 블루-그린 배포
  - 카나리 릴리스
  - 피처 플래그 기반 배포
- [] 비즈니스 임팩트 분석
  - 시스템 다운타임 비용 계산
  - SLA 위반 비용 분석
  - 비즈니스 가치 메트릭

### 사용자 경험 혁신 (User Experience Innovation)
- [] 자연어 인터페이스
  - 음성 기반 큐 관리
  - 챗봇을 통한 시스템 상호작용
  - 자연어 쿼리 지원
- [] 증강 현실 (AR) 모니터링
  - 3D 큐 시각화
  - AR 기반 시스템 상태 표시
  - 몰입형 데이터 탐색
- [] 개인화된 대시보드
  - 사용자별 맞춤형 뷰
  - 역할 기반 UI 커스터마이제이션
  - 지능형 위젯 추천

## 기술 스택

### 백엔드
- **Spring Boot 3.2** (Kotlin)
- **Spring Data Redis**
- **Spring Web**
- **Jackson** (JSON 처리)
- **Kotlin Coroutines** (비동기 처리)

### 저장소
- **Redis 7.0+**
  - 메시지 저장소
  - 큐 메타데이터 저장
  - 분산 락 구현

### 스케줄링
- **Spring Scheduler**
  - DLQ 이동 작업
  - 만료 메시지 정리
  - 메트릭 수집

### 운영 도구
- **Docker Compose**
  - Redis, Application 컨테이너 관리
- **Gradle** (빌드 도구)
- **Micrometer** (메트릭)

## 시스템 아키텍처

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Producer      │    │   Queue Service │    │   Consumer      │
│   Application   │───▶│   (Spring Boot) │◀───│   Application   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                               │
                               ▼
                       ┌─────────────────┐
                       │     Redis       │
                       │  Message Store  │
                       └─────────────────┘
```

## API 명세

### 인증
- **API Key** 기반 인증
- Header: `X-API-Key: {tenant-api-key}`
- 모든 API 요청에 필수

### 공통 응답 형식
```json
{
  "success": true,
  "data": {},
  "error": {
    "code": "ERROR_CODE",
    "message": "에러 메시지"
  },
  "timestamp": "2025-06-17T10:30:00Z"
}
```

### 큐 관리 API

#### 1. 큐 생성
```http
POST /api/v1/queues
Content-Type: application/json
X-API-Key: {tenant-api-key}

{
  "queueName": "my-queue",
  "attributes": {
    "visibilityTimeoutSeconds": 30,
    "messageRetentionPeriod": 1209600,
    "delaySeconds": 0,
    "maxReceiveCount": 3,
    "deadLetterTargetArn": "my-dlq"
  }
}
```

#### 2. 큐 목록 조회
```http
GET /api/v1/queues?limit=20&offset=0&namePattern=test-*
X-API-Key: {tenant-api-key}
```

#### 3. 큐 속성 조회
```http
GET /api/v1/queues/{queueName}/attributes
X-API-Key: {tenant-api-key}
```

#### 4. 큐 삭제
```http
DELETE /api/v1/queues/{queueName}
X-API-Key: {tenant-api-key}
```

### 메시지 관리 API

#### 1. 메시지 전송
```http
POST /api/v1/queues/{queueName}/messages
Content-Type: application/json
X-API-Key: {tenant-api-key}

{
  "messageBody": "Hello World",
  "delaySeconds": 0,
  "attributes": {
    "customKey": "customValue"
  }
}
```

#### 2. 메시지 일괄 전송
```http
POST /api/v1/queues/{queueName}/messages/batch
Content-Type: application/json
X-API-Key: {tenant-api-key}

{
  "messages": [
    {
      "id": "msg1",
      "messageBody": "Message 1",
      "delaySeconds": 0
    },
    {
      "id": "msg2", 
      "messageBody": "Message 2",
      "delaySeconds": 5
    }
  ]
}
```

#### 3. 메시지 수신
```http
GET /api/v1/queues/{queueName}/messages?maxNumberOfMessages=10&visibilityTimeoutSeconds=30&waitTimeSeconds=20
X-API-Key: {tenant-api-key}
```

#### 4. 메시지 삭제
```http
DELETE /api/v1/queues/{queueName}/messages/{receiptHandle}
X-API-Key: {tenant-api-key}
```

### 모니터링 API

#### 큐 통계 조회
```http
GET /api/v1/queues/{queueName}/stats
X-API-Key: {tenant-api-key}
```

## 데이터 모델

### Queue Entity
```kotlin
data class Queue(
    val tenantId: String,
    val queueName: String,
    val attributes: QueueAttributes,
    val createdAt: Instant,
    val updatedAt: Instant
)

data class QueueAttributes(
    val visibilityTimeoutSeconds: Int = 30,
    val messageRetentionPeriod: Int = 1209600, // 14일
    val delaySeconds: Int = 0,
    val maxReceiveCount: Int = 3,
    val deadLetterTargetArn: String? = null,
    val redrivePolicy: RedrivePolicy? = null
)
```

### Message Entity  
```kotlin
data class Message(
    val messageId: String,
    val tenantId: String,
    val queueName: String,
    val messageBody: String,
    val attributes: Map<String, String> = emptyMap(),
    val sentTimestamp: Instant,
    val firstReceiveTimestamp: Instant? = null,
    val receiveCount: Int = 0,
    val visibleAt: Instant,
    val receiptHandle: String? = null
)
```

### Tenant Entity
```kotlin
data class Tenant(
    val tenantId: String,
    val apiKey: String,
    val name: String,
    val createdAt: Instant,
    val isActive: Boolean = true
)
```

## Redis 데이터 구조

### 1. 큐 메타데이터
```
Key: queue:{tenantId}:{queueName}
Type: Hash
Fields:
- name: 큐 이름
- visibilityTimeout: 가시성 타임아웃
- messageRetention: 메시지 보존 기간
- delaySeconds: 기본 지연 시간
- maxReceiveCount: 최대 수신 횟수
- deadLetterQueue: DLQ 이름
- createdAt: 생성 시간
```

### 2. 메시지 대기 큐 (Available)
```
Key: queue:messages:{tenantId}:{queueName}
Type: List (FIFO)
Value: messageId
```

### 3. 지연 메시지 (Delayed)
```
Key: queue:delayed:{tenantId}:{queueName}
Type: Sorted Set
Score: 실행 시간 (timestamp)
Value: messageId
```

### 4. 처리 중 메시지 (In-Flight)
```
Key: queue:inflight:{tenantId}:{queueName}
Type: Sorted Set  
Score: 가시성 복원 시간
Value: messageId:receiptHandle
```

### 5. 메시지 상세 정보
```
Key: message:{messageId}
Type: Hash
Fields:
- body: 메시지 내용
- attributes: 메시지 속성 (JSON)
- sentTimestamp: 전송 시간
- receiveCount: 수신 횟수
- firstReceiveTimestamp: 첫 수신 시간
```

### 6. 테넌트별 큐 목록
```
Key: tenant:queues:{tenantId}
Type: Set
Value: queueName
```

## 핵심 알고리즘

### 메시지 전송 (SendMessage)
1. 메시지 ID 생성 (UUID)
2. 메시지 상세 정보 저장
3. DelaySeconds > 0인 경우: delayed queue에 추가
4. DelaySeconds = 0인 경우: available queue에 추가
5. 큐 통계 업데이트

### 메시지 수신 (ReceiveMessage)
1. In-flight 큐에서 만료된 메시지 복원
2. Delayed 큐에서 실행 시간이 된 메시지를 available로 이동
3. Available 큐에서 메시지 poll
4. Receipt handle 생성 및 in-flight 큐에 추가
5. 메시지 수신 횟수 증가
6. MaxReceiveCount 초과시 DLQ로 이동

### 가시성 타임아웃 처리
- Spring Scheduler로 주기적 실행 (30초마다)
- In-flight 큐에서 타임아웃된 메시지 탐지
- Available 큐로 메시지 복원
- Receipt handle 무효화

## 에러 처리

### 에러 코드 정의
```kotlin
enum class ErrorCode(val message: String) {
    QUEUE_NOT_FOUND("큐를 찾을 수 없습니다"),
    QUEUE_ALREADY_EXISTS("큐가 이미 존재합니다"),
    MESSAGE_NOT_FOUND("메시지를 찾을 수 없습니다"),
    INVALID_RECEIPT_HANDLE("잘못된 receipt handle입니다"),
    MESSAGE_TOO_LARGE("메시지 크기가 너무 큽니다"),
    INVALID_PARAMETER("잘못된 파라미터입니다"),
    UNAUTHORIZED("인증 실패"),
    RATE_LIMIT_EXCEEDED("요청 한도 초과")
}
```

## 성능 고려사항

### Redis 최적화
- Connection Pooling 설정
- Pipeline 사용으로 네트워크 RTT 최소화
- 적절한 TTL 설정으로 메모리 사용량 관리

### 큐 처리량 최적화
- Batch 작업 지원
- Long Polling으로 불필요한 호출 감소
- 큐별 독립적인 처리를 위한 파티셔닝

### 모니터링 지표
- 큐별 메시지 수 (available, in-flight, delayed)
- 처리 지연 시간 (age of oldest message)
- 에러율 및 DLQ 메시지 수
- API 응답 시간

## 배포 및 운영

### Docker Compose 구성
```yaml
version: '3.8'
services:
  app:
    build: .
    ports:
      - "8080:8080"
    environment:
      - SPRING_REDIS_HOST=redis
    depends_on:
      - redis
      
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
      
volumes:
  redis_data:
```

### 환경 설정
```yaml
# application.yml
spring:
  redis:
    host: localhost
    port: 6379
    timeout: 2000ms
    lettuce:
      pool:
        max-active: 10
        max-idle: 5
        
app:
  queue:
    default-visibility-timeout: 30
    max-message-size: 262144  # 256KB
    max-messages-per-batch: 10
    scheduler-interval: 30000  # 30초
```

## 향후 확장 계획

### Phase 2
- [ ] 메시지 필터링 (Message Filtering)
- [ ] 큐 암호화 지원
- [ ] CloudWatch 스타일 메트릭 대시보드
- [ ] WebSocket을 통한 실시간 알림

### Phase 3  
- [ ] Multi-AZ 지원 (Redis Cluster)
- [ ] 메시지 중복 제거 (Deduplication)
- [ ] 트랜잭션 지원
- [ ] GraphQL API

이 스펙은 사이드 프로젝트로 시작하여 점진적으로 기능을 확장할 수 있도록 설계되었습니다. 핵심 SQS 기능을 모두 포함하면서도 구현 복잡도를 적절히 조절했습니다.



