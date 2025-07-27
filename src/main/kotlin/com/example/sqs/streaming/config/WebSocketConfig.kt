package com.example.sqs.streaming.config

import com.example.sqs.streaming.handler.StreamingWebSocketHandler
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.reactive.HandlerMapping
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping
import org.springframework.web.reactive.socket.WebSocketHandler
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter

@Configuration
class WebSocketConfig {

    @Bean
    fun handlerMapping(streamingHandler: StreamingWebSocketHandler): HandlerMapping {
        val mapping = SimpleUrlHandlerMapping()
        mapping.urlMap = mapOf("/ws/stream" to streamingHandler)
        mapping.order = -1 // 높은 우선순위
        return mapping
    }

    @Bean
    fun handlerAdapter(): WebSocketHandlerAdapter {
        return WebSocketHandlerAdapter()
    }
}