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
    
    // Test
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
    testImplementation("io.projectreactor:reactor-test")
    testImplementation("com.github.kstyrc:embedded-redis:0.6")
}

tasks.withType<Test> {
    useJUnitPlatform()
    
    // 테스트 실행 설정
    maxHeapSize = "1g"
    jvmArgs = listOf(
        "-XX:+UseG1GC",
        "-XX:MaxGCPauseMillis=100"
    )
    
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
        freeCompilerArgs += "-Xjsr305=strict"
        jvmTarget = "21"
    }
}