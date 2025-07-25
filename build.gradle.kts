import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    id("org.springframework.boot") version "3.2.6"
    id("io.spring.dependency-management") version "1.1.5"
    kotlin("jvm") version "1.9.24"
    kotlin("plugin.spring") version "1.9.24"
}

group = "com.example"
version = "0.0.1-SNAPSHOT"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("org.springframework.boot:spring-boot-starter-data-redis-reactive")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    
    // Kotlin
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor")
    implementation("io.projectreactor.kotlin:reactor-kotlin-extensions")
    
    // Redis (Reactive)
    implementation("io.lettuce:lettuce-core")
    
    // RocksDB for WARM storage layer
    implementation("org.rocksdb:rocksdbjni:8.11.3")
    
    // Monitoring
    implementation("io.micrometer:micrometer-registry-prometheus")
    
    // Utilities
    implementation("org.apache.commons:commons-lang3")
    
    // MessagePack for binary serialization (performance optimization)
    implementation("org.msgpack:msgpack-core:0.9.8")
    implementation("org.msgpack:jackson-dataformat-msgpack:0.9.8")
    
    // Test
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
    testImplementation("io.projectreactor:reactor-test")
    testImplementation("com.github.kstyrc:embedded-redis:0.6")
}

tasks.withType<Test> {
    useJUnitPlatform()
    
    // 테스트 실행 설정 (성능 최적화)
    maxHeapSize = "2g"  // 테스트 메모리 증가
    minHeapSize = "512m"
    jvmArgs = listOf(
        "-XX:+UseG1GC",
        "-XX:MaxGCPauseMillis=100",
        "-XX:+UseStringDeduplication"
    )
    
    // 병렬 테스트 설정
    maxParallelForks = Runtime.getRuntime().availableProcessors()
    
    // 테스트 리포트 설정
    reports {
        junitXml.required.set(true)
        html.required.set(true)
    }
    
    // 테스트 태그 설정
    systemProperty("junit.jupiter.execution.parallel.enabled", "true")
    systemProperty("junit.jupiter.execution.parallel.mode.default", "concurrent")
    
    // 테스트 분류
    testLogging {
        events("passed", "skipped", "failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        showExceptions = true
        showCauses = true
        showStackTraces = true
    }
}

// 성능 테스트 전용 태스크
tasks.register<Test>("performanceTest") {
    useJUnitPlatform()
    
    // 성능 테스트만 포함 (패키지 기반 필터링)
    filter {
        includeTestsMatching("*PerformanceTest*")
        includeTestsMatching("com.example.sqs.performance.*")
    }
    
    // 성능 테스트는 단독 실행
    maxParallelForks = 1
    
    // 더 많은 힙 메모리 할당
    maxHeapSize = "2g"
    
    group = "verification"
    description = "Runs performance tests"
    
    // 상세한 로깅
    testLogging {
        events("passed", "skipped", "failed", "standardOut", "standardError")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        showExceptions = true
        showCauses = true
        showStackTraces = true
    }
}

// 테스트 커버리지를 위한 JaCoCo 플러그인 (선택사항)
// apply(plugin = "jacoco")

tasks.withType<KotlinCompile> {
    kotlinOptions {
        freeCompilerArgs += listOf(
            "-Xjsr305=strict",
            "-Xopt-in=kotlin.RequiresOptIn",
            "-Xopt-in=kotlinx.coroutines.ExperimentalCoroutinesApi",
            "-Xopt-in=kotlinx.coroutines.FlowPreview"
        )
        jvmTarget = "21"
    }
}

// Build 성능 최적화
tasks.withType<org.springframework.boot.gradle.tasks.bundling.BootJar> {
    archiveFileName.set("${project.name}.jar")
    
    // JAR 최적화
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true
    
    // 레이어 최적화
    layered {
        enabled = true
    }
}

// Gradle 빌드 캐시 최적화
configurations.all {
    resolutionStrategy.cacheChangingModulesFor(0, "seconds")
    resolutionStrategy.cacheDynamicVersionsFor(0, "seconds")
}

// 메모리 최적화는 gradle.properties에서 설정