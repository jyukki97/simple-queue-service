# ColdStorageService 리팩토링

## 개요
ColdStorageService가 868줄의 거대한 클래스에서 역할별로 분리된 여러 서비스로 리팩토링되었습니다.

## 리팩토링 전후 비교

### 리팩토링 전
- **ColdStorageService.kt**: 868줄의 거대한 단일 클래스
- 모든 로직이 하나의 파일에 집중
- 테스트 및 유지보수 어려움

### 리팩토링 후
- **메인 서비스**: ColdStorageService.kt (약 150줄)
- **데이터 클래스**: 별도 패키지로 분리
- **기능별 서비스**: 역할에 따라 분리

## 파일 구조

```
src/main/kotlin/com/example/sqs/storage/
├── domain/cold/
│   ├── FileMetadata.kt        # 파일 메타데이터 정의
│   └── SystemMetrics.kt       # 시스템 메트릭 정의
├── service/cold/
│   ├── ColdStorageFileUtils.kt           # 파일 조작 유틸리티
│   ├── ColdStorageIntegrityService.kt    # 무결성 검증 서비스
│   ├── ColdStorageCompressionService.kt  # 압축 관리 서비스
│   ├── ColdStorageBatchService.kt        # 배치 처리 서비스
│   └── ColdStorageQueueService.kt        # 큐 검색 서비스
└── service/impl/
    └── ColdStorageService.kt    # 메인 서비스 (리팩토링됨)
```

## 분리된 서비스들

### 1. ColdStorageFileUtils
**역할**: 파일 시스템 조작 담당
- 파일 읽기/쓰기
- 압축 파일 처리
- 메타데이터 관리
- 아카이브 경로 생성

**주요 메소드**:
- `createArchivePath()`: 아카이브 경로 생성
- `readMessagesFromFile()`: 압축 파일에서 메시지 읽기
- `writeCompressedFile()`: 압축 파일 쓰기
- `saveMetadata()`, `readMetadata()`: 메타데이터 관리

### 2. ColdStorageIntegrityService
**역할**: 데이터 무결성 검증 담당
- SHA-256 체크섬 계산
- 파일 무결성 검증
- 스케줄링된 백그라운드 검증
- 손상된 파일 처리

**주요 메소드**:
- `calculateSHA256Async()`: 비동기 체크섬 계산
- `verifyFileIntegrity()`: 파일 무결성 검증
- `scheduleIntegrityCheck()`: 스마트 검증 스케줄링
- `performScheduledIntegrityChecks()`: 정기 검증 (@Scheduled)

### 3. ColdStorageCompressionService
**역할**: 압축 레벨 관리 담당
- 시스템 상태 기반 동적 압축 레벨 결정
- CPU 사용률에 따른 압축 최적화

**주요 메소드**:
- `determineCompressionLevel()`: 동적 압축 레벨 결정
- `getCompressionInfo()`: 압축 설정 정보 조회

### 4. ColdStorageBatchService
**역할**: 배치 처리 담당
- 원자적 배치 쓰기
- 체크섬과 함께 안전한 저장
- 압축 테스트 기능

**주요 메소드**:
- `saveBatch()`: 배치 저장 메인 로직
- `writeBatchAtomicWithChecksum()`: 체크섬과 함께 원자적 쓰기
- `testDynamicCompression()`: 압축 레벨 테스트

### 5. ColdStorageQueueService
**역할**: 큐 관련 검색 담당
- 큐별 메시지 수 조회
- 오래된 메시지 검색
- 큐 메시지 삭제

**주요 메소드**:
- `getMessageCountByQueue()`: 큐별 메시지 수 조회
- `getOldestMessageTime()`: 가장 오래된 메시지 시간 조회
- `deleteQueueMessages()`: 큐 메시지 삭제

## 데이터 클래스 분리

### FileMetadata
파일의 메타데이터를 담는 데이터 클래스로 분리:
- 파일 정보 (이름, 크기, 압축 비율)
- 메시지 정보 (개수, ID 범위, 시간 범위)
- 무결성 정보 (체크섬, 검증 상태)

### SystemMetrics
시스템 메트릭을 담는 데이터 클래스로 분리:
- CPU, 메모리, IO, 디스크 사용률
- 시뮬레이션된 메트릭 제공

## 메인 서비스 변경점

### ColdStorageService
- **의존성 주입**: 분리된 서비스들을 생성자 주입으로 받음
- **코드 단순화**: 각 메소드가 적절한 서비스에 위임
- **유지보수성 향상**: 핵심 인터페이스 구현에 집중

**변경된 메소드들**:
```kotlin
// 기존: 직접 구현
override suspend fun saveBatch(messages: List<Message>): BatchResult {
    // 100+ 줄의 복잡한 로직
}

// 리팩토링 후: 서비스 위임
override suspend fun saveBatch(messages: List<Message>): BatchResult {
    return batchService.saveBatch(basePath, messages)
}
```

## 장점

### 1. 단일 책임 원칙 (SRP)
- 각 서비스가 하나의 명확한 역할만 담당
- 코드 이해도 향상

### 2. 테스트 용이성
- 각 서비스를 독립적으로 테스트 가능
- Mock 객체 활용 용이

### 3. 재사용성
- 다른 스토리지 서비스에서 유틸리티 재사용 가능
- 공통 로직 중복 제거

### 4. 유지보수성
- 특정 기능 수정 시 해당 서비스만 수정
- 사이드 이펙트 최소화

### 5. 확장성
- 새로운 기능 추가 시 새로운 서비스 생성
- 기존 코드 변경 최소화

## Spring 통합

모든 서비스는 `@Component` 또는 `@Service`로 Spring 컨테이너에서 관리:
- 자동 의존성 주입
- 라이프사이클 관리
- 설정 값 주입 (`@Value`)

## 마이그레이션 가이드

기존 코드를 사용하는 경우:
1. 메인 서비스 인터페이스는 그대로 유지
2. 내부 구현만 리팩토링됨
3. 기존 테스트 코드 수정 필요 없음

## 향후 개선 방향

1. **인터페이스 추가**: 각 서비스에 대한 인터페이스 정의
2. **설정 클래스**: 압축, 무결성 등 설정을 별도 클래스로 분리
3. **메트릭 수집**: 실제 시스템 메트릭 수집 로직 구현
4. **에러 처리**: 더 세밀한 에러 처리 및 복구 로직
