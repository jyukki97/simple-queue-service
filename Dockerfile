FROM openjdk:21-jdk-slim

WORKDIR /app

# Gradle wrapper와 gradle 파일들 복사
COPY gradle gradle
COPY gradlew .
COPY build.gradle.kts .
COPY settings.gradle.kts .
COPY gradle.properties .

# 소스 코드 복사
COPY src src

# 애플리케이션 빌드
RUN ./gradlew bootJar --no-daemon

# 빌드된 JAR 파일을 실행 가능한 이름으로 복사
RUN cp build/libs/*.jar app.jar

# 실행
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
