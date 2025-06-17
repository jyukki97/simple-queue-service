package com.example.sqs.common.controller

import com.example.sqs.common.domain.ApiResponse
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/v1")
class HealthController {

    @GetMapping("/ping")
    fun ping(): ApiResponse<String> {
        return ApiResponse.success("pong")
    }
}
