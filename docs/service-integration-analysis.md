## FIFO vs Standard 큐 메시지 서비스 통합 분석

### 현재 상황
1. **파티션 구조 통일**: ✅ 완료
2. **저장 방식 통일**: ✅ JSON String 완료  
3. **키 패턴 통일**: ✅ 완료

### 남은 차이점

#### 1. 메시지 전송 로직
**Standard 큐 (UnifiedStorageService)**:
- 복잡한 티어링 매니저 사용
- 자동 승격/강등 로직
- 백그라운드 처리

**FIFO 큐 (FifoStorageService)**:
- 단순한 Hot Storage 직접 저장
- MessageGroupId 기반 파티션
- 즉시 처리

#### 2. 메시지 수신 로직
**Standard 큐 (RedisMessageRepository)**:
- 전통적인 단일 큐 방식
- 만료 메시지 복원
- 지연 메시지 처리

**FIFO 큐 (FifoMessageService)**:
- 파티션 기반 최적화된 검색
- Hot → Warm → Cold 순차 검색
- 자동 승격 로직

#### 3. 파티션 결정 로직
**Standard 큐**: 메시지 body 해시
**FIFO 큐**: MessageGroupId 해시 (순서 보장을 위해)

### 통합 가능성 분석

#### 완전 통합이 어려운 이유:
1. **FIFO 순서 보장**: MessageGroupId 기반 파티셔닝 필수
2. **수신 최적화**: FIFO는 파티션별 순차 처리가 중요
3. **성능 특성**: FIFO는 Hot Storage 중심, Standard는 티어링 중심

#### 부분 통합 가능 영역:
1. **메시지 저장 로직**: 공통 인터페이스 추출 가능
2. **파티션 관리**: 공통 유틸리티 클래스
3. **메시지 상세 저장/조회**: 동일한 JSON 처리

### 권장사항

#### Option 1: 현재 구조 유지 (권장)
- FIFO와 Standard의 본질적 차이를 고려
- 각 큐 타입의 최적화된 로직 유지
- 공통 유틸리티만 추출

#### Option 2: 인터페이스 통합
- MessageStorageService 인터페이스 생성
- FifoMessageStorageServiceImpl, StandardMessageStorageServiceImpl
- 메시지 전송/수신 로직 공통화

#### Option 3: 하이브리드 접근
- 공통 기능은 통합 (메시지 저장, 파티션 관리)
- 큐별 특수 로직은 분리 유지

### 결론
현재 상태에서는 **Option 1 (현재 구조 유지)**를 권장합니다.
파티션 구조와 저장 방식이 통일되었으므로 주요 목표는 달성되었고,
FIFO와 Standard 큐의 본질적 차이점(순서 보장, 성능 특성)을 고려할 때
완전 통합보다는 공통 유틸리티 추출이 더 적절합니다.
