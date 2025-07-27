package com.example.sqs.streaming.handler

import com.example.sqs.streaming.domain.*
import com.example.sqs.streaming.service.StreamingService
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import org.springframework.web.reactive.socket.WebSocketHandler
import org.springframework.web.reactive.socket.WebSocketMessage
import org.springframework.web.reactive.socket.WebSocketSession
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration

@Component
class StreamingWebSocketHandler(
    private val streamingService: StreamingService,
    private val objectMapper: ObjectMapper
) : WebSocketHandler {

    private val logger = LoggerFactory.getLogger(StreamingWebSocketHandler::class.java)

    override fun handle(session: WebSocketSession): Mono<Void> {
        // API 키에서 테넌트 ID 추출 (쿼리 파라미터 또는 헤더에서)
        val tenantId = extractTenantId(session)
            ?: return session.close().doOnSuccess {
                logger.warn("WebSocket connection rejected - missing tenant authentication")
            }

        logger.info("WebSocket connection attempt - sessionId: ${session.id}, tenant: $tenantId")

        // 연결 설정
        return streamingService.establishConnection(session, tenantId)
            .flatMap { connection ->
                val connectionId = connection.connectionId

                // 들어오는 메시지 처리
                val input = session.receive()
                    .map { it.payloadAsText }
                    .flatMap { payload ->
                        handleIncomingMessage(connectionId, payload)
                    }
                    .doOnError { error ->
                        logger.error("Error processing incoming message for connection $connectionId", error)
                    }
                    .onErrorResume { error ->
                        val errorResponse = StreamingResponse(
                            type = "ERROR",
                            success = false,
                            error = error.message ?: "알 수 없는 오류가 발생했습니다"
                        )
                        Mono.just(errorResponse)
                    }

                // 나가는 메시지 스트림
                val output = streamingService.getEventStream(connectionId)
                    .map { event ->
                        try {
                            val json = objectMapper.writeValueAsString(event)
                            session.textMessage(json)
                        } catch (e: Exception) {
                            logger.error("Error serializing event for connection $connectionId", e)
                            session.textMessage("""{"type":"ERROR","success":false,"error":"직렬화 오류"}""")
                        }
                    }
                    .mergeWith(
                        // 응답 메시지를 포함
                        input.map { response ->
                            try {
                                val json = objectMapper.writeValueAsString(response)
                                session.textMessage(json)
                            } catch (e: Exception) {
                                logger.error("Error serializing response for connection $connectionId", e)
                                session.textMessage("""{"type":"ERROR","success":false,"error":"직렬화 오류"}""")
                            }
                        }
                    )
                    .doOnComplete {
                        logger.info("WebSocket output stream completed for connection $connectionId")
                    }
                    .doOnError { error ->
                        logger.error("Error in WebSocket output stream for connection $connectionId", error)
                    }

                // 입출력 스트림 병합 및 연결 정리
                session.send(output)
                    .doFinally {
                        streamingService.closeConnection(connectionId).subscribe()
                    }
            }
            .doOnError { error ->
                logger.error("Error establishing WebSocket connection for tenant $tenantId", error)
            }
    }

    private fun handleIncomingMessage(connectionId: String, payload: String): Mono<StreamingResponse> {
        return try {
            val messageNode = objectMapper.readTree(payload)
            val type = messageNode.get("type")?.asText() ?: "UNKNOWN"

            when (type) {
                "SUBSCRIBE" -> {
                    val request = objectMapper.treeToValue(messageNode.get("data"), SubscribeRequest::class.java)
                    streamingService.subscribe(connectionId, request)
                }
                "UNSUBSCRIBE" -> {
                    val request = objectMapper.treeToValue(messageNode.get("data"), UnsubscribeRequest::class.java)
                    streamingService.unsubscribe(connectionId, request)
                }
                "PING" -> {
                    Mono.just(StreamingResponse(
                        type = "PONG",
                        success = true,
                        data = mapOf("timestamp" to System.currentTimeMillis())
                    ))
                }
                else -> {
                    Mono.just(StreamingResponse(
                        type = "ERROR",
                        success = false,
                        error = "알 수 없는 메시지 타입: $type"
                    ))
                }
            }
        } catch (e: Exception) {
            logger.error("Error parsing incoming WebSocket message for connection $connectionId: $payload", e)
            Mono.just(StreamingResponse(
                type = "ERROR",
                success = false,
                error = "메시지 파싱 오류: ${e.message}"
            ))
        }
    }

    private fun extractTenantId(session: WebSocketSession): String? {
        // 쿼리 파라미터에서 API 키 추출
        val apiKey = session.handshakeInfo.uri.query
            ?.split("&")
            ?.map { it.split("=") }
            ?.associate { it[0] to (it.getOrNull(1) ?: "") }
            ?.get("apiKey")

        // 실제 환경에서는 API 키로 테넌트 ID를 조회해야 함
        // 여기서는 간단히 하드코딩된 매핑 사용
        return when (apiKey) {
            "test-api-key" -> "test-tenant"
            "admin-api-key" -> "admin-tenant"
            else -> null
        }
    }
}