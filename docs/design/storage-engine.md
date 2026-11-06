# 저장 엔진 재설계

- 작성일: 2026-11-06
- 상태: 확정 (0단계 착수 전)

## 요약

- 겉 동작은 AWS SQS에 맞춘다. 저장소는 직접 만들어 큐 부하에 특화한다.
- 구현 언어는 Rust다. 코드는 AI로 짜고, 정확성은 테스트가 판정한다.
- API는 SQS 호환 프로토콜(AWS JSON 1.0)이다. AWS SDK를 그대로 붙인다.
- 엔진 인터페이스는 키-값 단위가 아니라 **큐 연산 단위**로 둔다. 그래야 나중에 큐 전용 엔진으로 바꿀 수 있다.
- 먼저 RocksDB 엔진으로 "껍질"을 완성한다. 껍질은 API, 큐 동작, 복제, 분할을 뜻한다.
- 그다음 같은 인터페이스 뒤에서 자체 엔진으로 바꾼다. 목표는 RocksDB의 약점만 없애는 것이다. 약점은 쓰기 증폭, 삭제 표시, 컴팩션 멈춤, 이중 쓰기다.
- 두 엔진은 같은 정확성 테스트와 같은 벤치마크로 비교한다.

| 단계 | 내용 | 끝났다고 보는 기준 |
|---|---|---|
| 0 | 측정 기반 | 엔진 벤치마크와 서비스 벤치마크가 µs 단위로 잰다. RocksDB 엔진은 기본 연산만 있다 |
| 1 | 껍질 + RocksDB 엔진 (한 대) | SQS 동작 테스트를 통과한다. Kotlin 구현을 대체한다 |
| 2 | 자체 엔진 v1 (한 대) | 같은 테스트를 통과한다. 벤치마크로 RocksDB 엔진과 비교한다 |
| 3 | 여러 노드 | 파티션을 노드에 나눠 맡기고 raft-rs로 복제한다 |
| 4 | 커밋 로그와 Raft 합치기 | 메시지 본문을 노드마다 한 번만 쓴다 |
| 5 | 자동 분할, 공정성, 셀 | 바쁜 큐는 파티션을 늘린다. 한 테넌트가 몰려도 다른 테넌트의 지연이 유지된다 |

## 저장소 구성

이 저장소의 루트에 Cargo workspace를 둔다.

| crate | 하는 일 | 단계 |
|---|---|---|
| `crates/sqs-server` | 앞단. SQS JSON API, SigV4 검증, 관리 REST(`/api/v1`) | 1 |
| `crates/sqs-core` | 큐 동작, 파티션, 라우팅 | 1 |
| `crates/sqs-engine` | `PartitionStore` trait과 공통 정확성 테스트 | 0 |
| `crates/sqs-engine-rocksdb` | RocksDB 엔진 | 0~1 |
| `crates/sqs-engine-native` | 자체 엔진 | 2 |
| `crates/sqs-bench` | 엔진 벤치마크와 서비스 벤치마크 | 0 |

- 관리 화면(`frontend/`)은 그대로 둔다. `/api/v1`만 바라보므로 앞단이 바뀌어도 영향이 없다.
- Kotlin 코드(`src/`, Gradle 파일)는 1단계를 통과하면 지운다.

## 목표와 범위

맞출 SQS 동작:

- Standard 큐: 최소 한 번 전달, 순서는 대략 보장
- FIFO 큐: 메시지 그룹 안에서 순서 보장, 중복 제거 5분
- visibility timeout(최대 12시간), visibility 변경, receipt handle
- DLQ와 최대 수신 횟수(maxReceiveCount)
- 지연 전송(최대 15분), 보관 기간(1분~14일)
- 롱 폴링(최대 20초), 배치(최대 10건)
- 메시지 크기 최대 1 MiB

API:

- AWS JSON 1.0 프로토콜을 받는다. 요청은 `POST /`, `X-Amz-Target: AmazonSQS.<Action>`, `Content-Type: application/x-amz-json-1.0`이다.
- 인증은 SigV4 서명을 검증한다. 테넌트마다 access key와 secret을 발급한다. access key가 테넌트를 가리킨다.
- `/api/v1` REST는 관리 화면 전용으로 남긴다.

남기는 것: 멀티 테넌트, 관리 화면(React).

범위 밖:

- AWS query 프로토콜. 이 프로토콜만 쓰는 오래된 SDK(예: AWS SDK for Java 2.x 2.21.19 미만, boto3 1.28.82 미만)는 붙지 않는다.
- Kafka식 스트리밍 모듈
- IAM 정책 수준의 권한, SNS 연동, KMS 암호화, 리전 간 복제

## 구조

```
클라이언트 (AWS SDK)
   │
앞단: API, 인증, 테넌트 쿼터, 롱 폴링 대기       ← 상태 없음 (axum, tokio)
   │
파티션 라우팅: 큐 → 파티션 → 노드                ← 메타데이터를 캐시
   │
파티션: 큐 동작, Raft 복제(3단계부터)
   │
엔진: PartitionStore                            ← RocksDB 엔진 / 자체 엔진
```

- 비동기 런타임은 tokio, HTTP는 axum이다.
- 엔진 호출은 동기다. 운영에서는 파티션마다 전용 스레드가 엔진을 부르고, 앞단과는 채널로 주고받는다. 결정적 시뮬레이션에서는 이 루프를 시뮬레이터 태스크로 돌린다(측정과 테스트 절).
- 롱 폴링 대기와 도착 알림은 앞단이 맡는다. 엔진은 기다리지 않는다.
- 파티션 하나가 큐 여러 개를 담는다. 작은 큐 수백만 개를 담기 위해서다.

## 엔진 인터페이스

```rust
pub trait PartitionStore: Send {
    fn create_queue(&mut self, queue: QueueId, attrs: &QueueAttributes) -> Result<()>;
    fn delete_queue(&mut self, queue: QueueId) -> Result<()>;
    fn send(&mut self, queue: QueueId, msgs: &[NewMessage], now: Millis) -> Result<Vec<SendResult>>;
    fn receive(&mut self, queue: QueueId, max: usize, visibility: Millis, now: Millis) -> Result<Vec<Received>>;
    fn change_visibility(&mut self, queue: QueueId, receipt: &Receipt, visibility: Millis, now: Millis) -> Result<()>;
    fn delete(&mut self, queue: QueueId, receipts: &[Receipt]) -> Result<Vec<DeleteResult>>;
    fn purge(&mut self, queue: QueueId) -> Result<()>;
    fn expire(&mut self, now: Millis) -> Result<()>;           // 보관 기간이 지난 메시지 정리
    fn stats(&self, queue: QueueId) -> Result<QueueStats>;      // 대략값
    fn snapshot(&self, target: &Path) -> Result<()>;            // Raft 스냅샷
    fn restore(&mut self, source: &Path) -> Result<()>;
}
```

규칙:

- 시각은 모두 인자로 받는다. 엔진은 시계를 직접 읽지 않는다. Raft 상태 기계가 결정적이어야 하고, 테스트도 쉬워진다.
- 한 파티션의 쓰기 연산은 한 스레드가 차례로 부른다. `receive`도 lease를 거는 쓰기다. Raft의 apply 모양과 같다.
- receipt에는 (큐, 순번, lease 세대)를 담는다. 다시 받을 때마다 세대가 올라간다.
- 오래된 세대의 receipt로 지우면 성공으로 응답하되 지우지 않는다. SQS DeleteMessage 문서가 적은 동작이다. 다만 SQS는 만료된 receipt에 `InvalidParameterValue`(The receipt handle has expired)를 돌려주기도 한다. 큐 종류(Standard·FIFO)별로 어느 응답을 내는지는 1단계에서 실제 SQS로 확인해 맞춘다.
- 형식이 틀리거나 다른 큐의 receipt는 `ReceiptHandleIsInvalid`로 거절한다.
- 복제가 없을 때는 엔진이 반환 전에 fsync까지 책임진다. 복제가 있을 때는 Raft가 내구성을 책임진다.

## RocksDB 엔진 (0~1단계)

- `rocksdb` crate(rust-rocksdb 프로젝트)를 쓴다. 공식 RocksDB(facebook/rocksdb)를 내장한다. TiKV는 이를 갈라 낸 `tikv/rust-rocksdb`와 자체 RocksDB 포크를 쓰므로 고르지 않는다.
- 0단계에는 벤치마크에 쓸 기본 연산(보내기, 받기, 지우기)만 만든다. 1단계에서 나머지를 채운다.

### 키 설계

| 접두 | 키 | 값 | 용도 |
|---|---|---|---|
| `q` | 큐 | 큐 속성, 다음 순번, 시작 위치 | 큐 메타데이터 |
| `m` | 큐, 순번 | 본문, 메시지 속성 | 메시지 본문 |
| `s` | 큐, 순번 | 받은 횟수, lease 세대, 보낸 시각 | 메시지 상태 |
| `v` | 큐, visible_at, 순번 | 빈 값 | 받을 수 있는 시각 색인 (지연·lease 포함) |
| `g` | 큐, 그룹, 순번 | 빈 값 | FIFO 그룹 순서 |
| `d` | 큐, 중복 제거 ID | 만료 시각 | FIFO 중복 제거 |

- 본문(`m`)과 상태(`s`)를 나눈다. 받을 때마다 상태가 바뀌는데, 그때 1 MiB 본문을 다시 쓰지 않기 위해서다.
- 숫자는 빅엔디언으로 넣어 바이트 순서와 값 순서를 맞춘다.

### 받기

1. 큐의 시작 위치 `(visible_at, 순번)`에서 `v`를 찾는다. 위쪽 경계는 현재 시각이다.
2. 찾은 메시지마다 한 쓰기 묶음으로 처리한다. `v`를 지우고 `v(현재 시각 + timeout)`을 넣고 `s`를 갱신한다.
3. 받은 횟수가 maxReceiveCount를 넘으면 DLQ로 옮긴다. DLQ가 같은 파티션이면 한 쓰기 묶음으로 옮긴다. 다른 파티션이면 DLQ 파티션에 먼저 쓰고 원래 파티션에서 지운다. 그 사이에 실패하면 중복이 생길 수 있는데, 최소 한 번 전달이라 허용한다.
4. 처음 찾은 키를 새 시작 위치로 기록한다.

시작 위치 규칙: 시작 위치보다 앞에 들어가는 쓰기가 있으면 시작 위치를 그 키로 당긴다. visibility를 0으로 되돌릴 때가 그런 경우다.

시작 위치를 기록하지 않으면, 처리가 밀린 큐를 비우는 처리량이 75배 떨어지고 받기 p50은 100배 넘게 느려진다(부록 B). 지운 키가 삭제 표시로 남아 큐 앞에 쌓이기 때문이다. RocksDB 위키도 같은 문제를 경고한다.

### 그 밖의 연산

- **FIFO:** 처리 중인 메시지가 있는 그룹은 건너뛴다. 그룹별 순서는 `g`로 지킨다. 세부 절차는 1단계에서 정한다.
- **보관 기간 만료:** 순번은 보낸 시각 순서다. 큐마다 경계 순번을 찾아 `m`과 `s`를 범위 삭제로 지운다. 남은 `v`는 받기에서 본문이 없으면 지운다.
- **큰 본문:** BlobDB(키·값 분리)를 켠다. 기준 크기는 측정해서 정한다.
- **내구성:** 응답 전에 WAL을 fsync한다. 동시에 들어온 쓰기는 한 번의 fsync로 묶는다.

## 자체 엔진 (2·4단계)

| 구성 | 하는 일 | 비슷한 사례 |
|---|---|---|
| 공용 커밋 로그 (파티션당 1개) | 그 파티션에 든 모든 큐의 메시지를 들어온 순서로 이어 쓴다. 세그먼트 파일로 나눈다 | RocketMQ CommitLog, RabbitMQ Ra WAL |
| 큐별 색인 | (로그 위치, 크기, visible_at)만 둔다. 큐가 늘어도 큐마다 로그 파일을 따로 만들지 않는다 | RocketMQ ConsumeQueue |
| 상태 기록 | lease와 처리 완료를 로그에 작은 기록으로 남긴다. 메모리 색인은 스냅샷과 로그로 다시 만든다 | 없음 |
| 세그먼트 정리 | 살아 있는 메시지가 적은 세그먼트는 살아 있는 것만 옮기고 버린다 | Bitcask merge, RocksDB BlobDB GC |

4단계에서는 커밋 로그가 raft-rs의 로그 저장(`Storage` trait)을 구현한다. 그래서 본문을 Raft 로그에 따로 한 번 더 쓰지 않는다.

없애려는 RocksDB 약점:

| 약점 | RocksDB 엔진 | 자체 엔진 |
|---|---|---|
| 쓰기 증폭 | 처리가 밀리면 컴팩션이 본문을 여러 번 다시 쓴다 | 본문은 한 번 쓴다. 정리 때 살아 있는 것만 옮긴다 |
| 삭제 표시 | 쌓이면 탐색이 느려진다. 시작 위치로 피한다 | 없다. 처리 완료는 상태 기록이다 |
| 컴팩션 멈춤 | 쓰기가 오래 몰리면 p99가 튈 수 있다 | 정리 속도를 직접 조절한다 |
| 이중 쓰기 | Raft 로그와 RocksDB에 두 번 쓴다 | 커밋 로그 하나에만 쓴다 |

어려운 점:

- 장애 일관성: 기록마다 체크섬, fsync 순서, 반쯤 쓴 기록 잘라 내기, 복구
- fsync 묶기(group commit)
- 메모리 색인 크기: 메시지당 수십 바이트가 든다. 차가운 큐의 색인은 디스크로 내린다. 이 부분만 RocksDB를 쓰는 안도 있다.

합격 기준:

- 공통 정확성 테스트를 모두 통과한다.
- 강제 종료 반복 테스트에서 처리 완료된 메시지가 다시 나오지 않고, 응답을 받은 메시지가 사라지지 않는다.
- 같은 벤치마크에서 처리량은 같거나 높고, p99와 디스크 쓰기 바이트는 낮다.

## 여러 노드 (3단계)

- StatefulSet으로 노드를 띄운다. 파티션 하나가 Raft 그룹 하나다.
- Raft는 raft-rs를 쓴다. etcd raft를 옮긴 것이라 로그 저장과 노드 사이 전송을 앱이 구현한다. TiKV가 운영에서 쓴다. 노드 하나가 Raft 그룹 여러 개를 함께 돌린다(multi-Raft).
- 큐→파티션→노드 배치는 메타데이터 저장소에 둔다. 앞단은 이를 캐시해서 라우팅한다.
- Standard 큐는 여러 파티션에 흩어질 수 있다. 받을 때 여러 파티션에서 모은다.
- FIFO 큐는 메시지 그룹 ID의 해시로 파티션을 정한다.
- 시각은 리더가 로그 항목에 찍은 값을 쓴다. 엔진 인터페이스의 `now`가 이 값이다.

## 개발 규칙

코드는 AI로 짠다. 사람은 설계와 테스트 결과를 본다.

- `unsafe`는 디스크 I/O 모듈에만 허용한다. 그 모듈은 따로 검토한다.
- crate 버전을 고정한다. AI가 옛 API로 짜는 일을 줄이기 위해서다.
- `cargo clippy`와 `cargo test`가 통과해야 커밋한다.

## 측정과 테스트

- **공통 정확성 테스트:** `PartitionStore` 계약을 테스트하는 묶음이다. 두 엔진에 같은 테스트를 돌린다.
- **장애 테스트:** 쓰는 도중 프로세스를 강제로 죽이고 다시 띄우기를 반복한다.
- **결정적 시뮬레이션:** turmoil이나 madsim으로 네트워크와 시간을 흉내 내고, 같은 시드로 같은 실행을 재현한다.
  - 시뮬레이션에서는 파티션 루프를 전용 OS 스레드가 아니라 시뮬레이터 태스크로 돌린다. madsim은 시스템 스레드 생성을 막고, turmoil은 한 스레드에서 돈다.
  - RocksDB 엔진은 자체 백그라운드 스레드와 파일 I/O가 시뮬레이터 밖에 있어 결정적이지 않다. 그래서 결정적 시뮬레이션은 테스트용 메모리 엔진과, 파일 I/O를 시뮬레이터 파일 시스템으로 돌리는 자체 엔진으로 한다.
  - 2단계부터 디스크 오류 주입을 더한다.
- **동시성 검사:** 앞단과 파티션 스레드 사이의 동시성 코드는 loom으로 실행 순서를 바꿔 가며 검사한다.
- **엔진 벤치마크:** `benchmarks/storage/`의 시나리오를 `crates/sqs-bench`로 옮긴다. 처리가 밀린 큐를 비우는 속도를 잰다. 부록 B(rocksdbjni 8.11.3)와 바로 비교하지 않는다. Rust `rocksdb` crate가 내장한 RocksDB 버전에 맞춰 JNI 쪽도 같은 버전으로 다시 잰 뒤 비교한다.
- **서비스 벤치마크:** `crates/sqs-bench`가 AWS SDK for Rust(`aws-sdk-sqs`, JSON 1.0으로 보낸다)로 부하를 건다. 비교 대상은 ElasticMQ다. `benchmark-suite.sh`는 초 단위로 재고 `curl`을 최대 1000개씩 띄워서 이것으로 바꾼다.

## 부록 A. 지금 구조의 문제

| 문제 | 결과 | 위치 |
|---|---|---|
| 오래된 메시지부터 Hot 밖으로 내린다 | 큐는 오래된 것부터 읽으므로, 처리가 밀릴수록 다음에 읽을 메시지부터 Hot 밖으로 나간다 | `storage/service/StandardTieringPolicyImpl.kt:91` |
| Hot 밖으로 내려간 메시지가 받기 경로에서 빠진다 | Standard 큐는 Hot 한도(큐당 100건 또는 2시간)를 넘겨 Warm으로 내려간 메시지를 받을 수 없다. 받기 목록에서 지우고 다시 넣지 않기 때문이다. FIFO 큐는 Warm에서 되돌리지만, Cold(1000건 또는 24시간)에서 받기는 구현되지 않았다 | `storage/service/WarmTierManagerService.kt:233`, `message/service/UnifiedMessageService.kt:213` |
| Standard 큐가 LIFO다 (`rightPop`) | 처리가 밀리면 오래된 메시지가 계속 뒤로 밀린다 | `message/service/UnifiedMessageService.kt:147` |
| Warm·Cold가 로컬 디스크이고 계층 이동에 잠금이 없다 | 여러 대로 띄우면 다른 노드가 옮긴 메시지를 볼 수 없고, 같은 메시지를 동시에 옮긴다 | `storage/scheduler/TieringScheduler.kt` |
| 최대 메시지 크기가 256KB다 | SQS(1 MiB)보다 작다 | `application.yml` `app.queue.max-message-size` |
| WebFlux 의존성이 있지만 Tomcat으로 뜬다 | `spring-boot-starter-websocket`이 서블릿 스택을 함께 끌어온다 | `build.gradle.kts` |

## 부록 B. 측정 결과

환경: Apple M4 Pro(12코어), Java 21, rocksdbjni 8.11.3, 메시지 256B, 복제 없음, RocksDB는 fsync 안 함.

한 번 호출 지연 (클라이언트 1개, 10만 건, `Bench.java`):

| 연산 | 평균 | p99 |
|---|---|---|
| RocksDB 쓰기 (같은 JVM) | 1.9µs | 4.4µs |
| RocksDB 읽기 (memtable) | 0.96µs | 2.1µs |
| RocksDB 읽기 (SST, 캐시 적중) | 1.2µs | 4.0µs |
| Redis SET (같은 컨테이너, AOF 1초마다 fsync) | 42µs | 71µs |
| Redis GET | 39µs | 55µs |

큐 부하 (받기 10건 + 처리 완료, 밀린 큐 비우기, `QueueBench.java`):

| 경우 | 처리량 | 받기 p50 |
|---|---|---|
| 큐 1000개 × 1000건, 매번 큐 처음부터 찾기 | 초당 12만 건 | 66µs |
| 같은 조건, 시작 위치 기록 | 초당 29만 건 | 21µs |
| 같은 조건, 시작 위치 기록, 8스레드 | 초당 38만~57만 건 (실행마다 흔들림) | 65~101µs |
| 큐 1개 × 20만 건, 매번 처음부터 찾기 | 초당 4천 건 (15초에서 끊음, 약 6만 건까지의 값) | 2.4ms |
| 같은 조건, 시작 위치 기록 | 초당 30만 건 | 19µs |

재현 방법:

JDK 21로 돌린다. 이 저장소의 Gradle 8.5는 JDK 22 이상에서 돌지 않는다. 이 방법은 Kotlin 빌드가 남아 있는 동안만 쓸 수 있다. 0단계에서 `crates/sqs-bench`로 옮긴다.

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 21)   # macOS 예
./gradlew bootJar
unzip -o -j build/libs/simple-queue-service.jar 'BOOT-INF/lib/rocksdbjni-*.jar' -d build/bench-lib
rm -rf build/bench-single && "$JAVA_HOME/bin/java" -cp "build/bench-lib/*" benchmarks/storage/Bench.java build/bench-single   # 한 번 호출 지연
"$JAVA_HOME/bin/java" -cp "build/bench-lib/*" benchmarks/storage/QueueBench.java build/bench-db                              # 큐 부하
```

Redis는 AOF를 켠 임시 컨테이너(`docker run --rm redis:7-alpine redis-server --appendonly yes --appendfsync everysec --save ''`) 안에서 `redis-benchmark -t set,get -n 100000 -d 256 -c 1 -P 1`로 쟀다.

## 부록 C. 구현 언어 비교

| | JVM (Kotlin) | Go | Rust |
|---|---|---|---|
| 1단계 LSM 엔진 | RocksDB (JNI) | Pebble (순수 Go) | RocksDB (`rocksdb` crate) |
| Raft | Apache Ratis(본문을 상태 기계 데이터 API로 Raft 로그 밖에 써야 한다) 또는 SOFAJRaft(로그 저장을 바꿔 끼운다) | etcd raft. 로그 저장을 앱이 구현한다 | raft-rs 또는 openraft. 로그 저장을 앱이 구현한다 |
| 자체 엔진의 메모리 색인 | GC 대상이다. 수억 건이면 기본형 배열이나 힙 밖 메모리(FFM API, Java 22+)로 짜야 GC 부담이 준다 | GC 대상이다. 포인터 없는 배열로 짜야 GC 부담이 준다 | GC가 없다. 메모리 배치를 직접 정한다 |
| 꼬리 지연 | ZGC로 GC 멈춤이 1ms 아래다 | GC 멈춤은 짧지만, 할당이 많으면 지연이 는다 | GC 영향이 없다 |
| 디스크 I/O 제어 | direct I/O는 된다(JDK 10+). io_uring은 외부 라이브러리로 쓴다 | direct I/O는 된다(`O_DIRECT`). io_uring은 외부 라이브러리로 쓴다 | direct I/O와 io_uring을 런타임 제약 없이 바로 쓸 수 있다 |
| 개발 속도 (AI로 개발) | 차이가 작다 | 차이가 작다 | 차이가 작다. 빌드가 가장 느리다 |
| AI가 짠 코드의 실수 | 데이터 경쟁은 테스트로 찾아야 한다 | 데이터 경쟁은 `-race`와 테스트로 찾아야 한다 | 메모리·데이터 경쟁 실수를 컴파일러가 막는다 |
| 메모리·배포 | 기본 수백 MB | 수십 MB 정적 바이너리 | 수십 MB 바이너리 |
| 비슷한 시스템 | Kafka, Pulsar, RocketMQ | NATS JetStream, etcd, CockroachDB | TiKV, Apache Iggy |

Rust를 고른 까닭:

- 2단계 자체 엔진이 이 프로젝트의 핵심이다. 메시지 수억 건의 색인과 fsync 순서를 직접 다뤄야 한다. GC가 없고 메모리 배치를 정할 수 있는 언어가 가장 유리하다.
- 4단계에 필요한 "로그 저장을 앱이 구현하는 Raft"(raft-rs)가 있다. Go(etcd raft)도 같아서 이것으로 언어가 갈리지는 않는다.
- 1단계 엔진이 부록 B에서 잰 RocksDB와 같아서, 버전만 맞추면 기준선이 이어진다.
- 코드는 AI로 짜므로 언어를 익히는 비용이 대가가 아니다. 남는 일은 검증이고, 컴파일러가 그 일의 일부를 맡는다.

뺀 언어:

- C++(Redpanda, ScyllaDB)는 메모리 안전성과 빌드 부담이 크다.
- Zig(TigerBeetle)는 언어가 1.0 전이고, 운영에서 검증된 Raft 라이브러리가 없다. TigerBeetle은 Raft가 아니라 자체 VSR을 쓴다.

## 부록 D. 참고한 자료

| 자료 | 참고한 점 |
|---|---|
| [Meta FOQS](https://engineering.fb.com/2021/02/22/production-engineering/foqs-scaling-a-distributed-priority-queue/) | 샤딩한 MySQL 위의 큐, 하루 약 1조 건(2021년 발표 기준) |
| [Apple QuiCK](https://pierrez.github.io/fdb-book/the-record-layer/quick.html) | FoundationDB 위의 큐, 테넌트별 큐 수백억 개, lease = 보이는 시각 미루기 |
| [Uber Cherami](https://www.uber.com/blog/cherami-message-queue-system/) | 데이터 묶음마다 RocksDB |
| [RabbitMQ quorum queue](https://www.rabbitmq.com/docs/quorum-queues) | 노드 공용 WAL + 큐별 세그먼트, 큐 수 한계 |
| [RocksDB로 큐 서비스 만들기](https://github.com/facebook/rocksdb/wiki/Implement-Queue-Service-Using-RocksDB) | 큐 앞에 쌓이는 삭제 표시와 대처법 |
| [Ratis 발표 (2018)](https://www.slideshare.net/Hadoop_Summit/high-throughput-data-replication-over-raft) | 상태 기계 데이터를 Raft 로그 밖에 써서 이중 쓰기를 피함 |
| [SQS 내부 구조 (AWS 블로그)](https://aws.amazon.com/blogs/aws/optimizing-amazon-simple-queue-service-sqs-for-speed-and-scale/) | 앞단과 저장 백엔드(Standard 큐), 셀 구조 |
| [SQS FIFO 파티션](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/high-throughput-fifo.html) | 그룹 ID 해시, 파티션 자동 증감 |
| [SQS 폴링](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html) | 메시지를 여러 서버에 흩어 두고 짧은 폴링은 일부만 조회 |
| [SQS DeleteMessage](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessage.html) | 오래된 receipt로 지우면 성공 응답, 지워지지 않을 수 있음 |
| [SQS JSON 프로토콜 FAQ](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-json-faqs.html) | JSON 프로토콜을 쓰는 SDK 최소 버전. FAQ는 Kotlin·Rust·Swift를 미지원으로 적었지만 오래된 문장이다. 지금은 이 SDK들의 SQS 클라이언트도 JSON 1.0으로 보낸다 |
| [SQS 최대 1 MiB](https://aws.amazon.com/about-aws/whats-new/2025/08/amazon-sqs-max-payload-size-1mib) | 메시지 크기 한도 |
