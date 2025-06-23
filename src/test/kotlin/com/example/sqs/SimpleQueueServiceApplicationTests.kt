package com.example.sqs

import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@org.springframework.test.context.ActiveProfiles("test")
class SimpleQueueServiceApplicationTests {

    @Test
    fun contextLoads() {
        // Spring context가 정상적으로 로드되는지 확인
    }
}
