package com.example.sqs.streaming.controller

import com.example.sqs.auth.domain.AuthContext
import com.example.sqs.common.domain.ApiResponse
import com.example.sqs.streaming.service.StreamingService
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono

@RestController
@RequestMapping("/api/v1/streaming")
class StreamingController(
    private val streamingService: StreamingService
) {

    /**
     * 활성 스트리밍 연결 상태 조회
     */
    @GetMapping("/status")
    fun getStreamingStatus(): Mono<ResponseEntity<ApiResponse<Map<String, Any>>>> {
        val tenantId = AuthContext.getCurrentTenantId()
        
        return Mono.fromCallable {
            val totalConnections = streamingService.getActiveConnectionCount()
            val tenantConnections = streamingService.getActiveConnectionCount(tenantId)
            
            val statusData = mapOf(
                "totalConnections" to totalConnections,
                "tenantConnections" to tenantConnections,
                "webSocketEndpoint" to "/ws/stream?apiKey=YOUR_API_KEY"
            )
            
            ResponseEntity.ok(
                ApiResponse.success(
                    data = statusData,
                    message = "스트리밍 상태 조회 성공"
                )
            )
        }
    }

    /**
     * 스트리밍 연결 정보 및 사용법 안내
     */
    @GetMapping("/info")
    fun getStreamingInfo(): Mono<ResponseEntity<ApiResponse<Map<String, Any>>>> {
        return Mono.fromCallable {
            val infoData = mapOf(
                "webSocketEndpoint" to "/ws/stream",
                "authentication" to "Query parameter: ?apiKey=YOUR_API_KEY",
                "supportedEvents" to listOf(
                    "MESSAGE_SENT",
                    "MESSAGE_RECEIVED", 
                    "MESSAGE_DELETED",
                    "QUEUE_CREATED",
                    "QUEUE_DELETED",
                    "QUEUE_PURGED",
                    "HEARTBEAT"
                ),
                "messageFormat" to mapOf(
                    "subscribe" to mapOf(
                        "type" to "SUBSCRIBE",
                        "data" to mapOf(
                            "queueNames" to listOf("queue1", "queue2"),
                            "eventTypes" to listOf("MESSAGE_SENT", "MESSAGE_RECEIVED"),
                            "filterExpression" to "optional",
                            "batchSize" to 1,
                            "maxWaitTime" to 10000
                        )
                    ),
                    "unsubscribe" to mapOf(
                        "type" to "UNSUBSCRIBE",
                        "data" to mapOf(
                            "subscriptionId" to "subscription-id"
                        )
                    ),
                    "ping" to mapOf(
                        "type" to "PING"
                    )
                ),
                "exampleUsage" to mapOf(
                    "javascript" to """
                        const ws = new WebSocket('ws://localhost:8080/ws/stream?apiKey=test-api-key');
                        
                        ws.onopen = () => {
                            // 메시지 전송 이벤트 구독
                            ws.send(JSON.stringify({
                                type: 'SUBSCRIBE',
                                data: {
                                    queueNames: ['my-queue'],
                                    eventTypes: ['MESSAGE_SENT']
                                }
                            }));
                        };
                        
                        ws.onmessage = (event) => {
                            const message = JSON.parse(event.data);
                            console.log('Received:', message);
                        };
                    """.trimIndent()
                )
            )
            
            ResponseEntity.ok(
                ApiResponse.success(
                    data = infoData,
                    message = "스트리밍 정보 조회 성공"
                )
            )
        }
    }
}