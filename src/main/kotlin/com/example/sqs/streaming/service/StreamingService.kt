package com.example.sqs.streaming.service

import com.example.sqs.auth.domain.AuthContext
import com.example.sqs.common.service.BaseReactiveService
import com.example.sqs.streaming.domain.*
import org.springframework.stereotype.Service
import org.springframework.web.reactive.socket.WebSocketSession
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.ConcurrentHashMap

@Service
class StreamingService : BaseReactiveService() {

    private val connections = ConcurrentHashMap<String, StreamingConnection>()
    private val sessions = ConcurrentHashMap<String, WebSocketSession>()
    private val eventSinks = ConcurrentHashMap<String, Sinks.Many<StreamingEvent>>()

    override fun getServiceName(): String = "StreamingService"

    /**
     * WebSocket 연결을 설정합니다
     */
    fun establishConnection(session: WebSocketSession, tenantId: String): Mono<StreamingConnection> {
        val connectionId = UUID.randomUUID().toString()
        val connection = StreamingConnection(
            connectionId = connectionId,
            tenantId = tenantId,
            sessionId = session.id
        )

        return Mono.fromCallable {
            connections[connectionId] = connection
            sessions[connectionId] = session
            
            // 이벤트 스트림 생성
            val sink = Sinks.many().multicast().onBackpressureBuffer<StreamingEvent>()
            eventSinks[connectionId] = sink

            logger.info("WebSocket connection established - connectionId: $connectionId, tenant: $tenantId")
            connection
        }
        .doOnNext { 
            // 연결 이벤트 발송
            publishEvent(StreamingEvent(
                eventId = UUID.randomUUID().toString(),
                eventType = StreamingEventType.CONNECTION_ESTABLISHED,
                tenantId = tenantId,
                queueName = "*",
                data = mapOf("connectionId" to connectionId)
            ))
        }
    }

    /**
     * WebSocket 연결을 종료합니다
     */
    fun closeConnection(connectionId: String): Mono<Void> {
        return Mono.fromRunnable<Void> {
            val connection = connections.remove(connectionId)
            sessions.remove(connectionId)
            eventSinks.remove(connectionId)?.tryEmitComplete()

            if (connection != null) {
                logger.info("WebSocket connection closed - connectionId: $connectionId, tenant: ${connection.tenantId}")
                
                // 연결 종료 이벤트 발송
                publishEvent(StreamingEvent(
                    eventId = UUID.randomUUID().toString(),
                    eventType = StreamingEventType.CONNECTION_LOST,
                    tenantId = connection.tenantId,
                    queueName = "*",
                    data = mapOf("connectionId" to connectionId)
                ))
            }
        }
    }

    /**
     * 큐에 구독을 추가합니다
     */
    fun subscribe(connectionId: String, request: SubscribeRequest): Mono<StreamingResponse> {
        return Mono.fromCallable {
            val connection = connections[connectionId] 
                ?: throw IllegalArgumentException("연결을 찾을 수 없습니다: $connectionId")

            val subscriptionId = UUID.randomUUID().toString()
            val subscription = StreamingSubscription(
                subscriptionId = subscriptionId,
                tenantId = connection.tenantId,
                queueNames = request.queueNames,
                eventTypes = request.eventTypes,
                filterExpression = request.filterExpression,
                batchSize = request.batchSize,
                maxWaitTime = request.maxWaitTime
            )

            connection.subscriptions.add(subscription)
            connections[connectionId] = connection

            logger.info("Subscription created - connectionId: $connectionId, subscriptionId: $subscriptionId, queues: ${request.queueNames}")

            StreamingResponse(
                type = "SUBSCRIBE_SUCCESS",
                success = true,
                data = mapOf(
                    "subscriptionId" to subscriptionId,
                    "queueNames" to request.queueNames,
                    "eventTypes" to request.eventTypes
                )
            )
        }
    }

    /**
     * 구독을 취소합니다
     */
    fun unsubscribe(connectionId: String, request: UnsubscribeRequest): Mono<StreamingResponse> {
        return Mono.fromCallable {
            val connection = connections[connectionId] 
                ?: throw IllegalArgumentException("연결을 찾을 수 없습니다: $connectionId")

            val removed = connection.subscriptions.removeIf { it.subscriptionId == request.subscriptionId }
            connections[connectionId] = connection

            logger.info("Subscription removed - connectionId: $connectionId, subscriptionId: ${request.subscriptionId}, success: $removed")

            StreamingResponse(
                type = "UNSUBSCRIBE_SUCCESS",
                success = removed,
                data = mapOf("subscriptionId" to request.subscriptionId)
            )
        }
    }

    /**
     * 메시지 전송 이벤트를 발송합니다
     */
    fun publishMessageSent(tenantId: String, queueName: String, messageId: String, messageBody: String, attributes: Map<String, String> = emptyMap()): Mono<Void> {
        val event = StreamingEvent(
            eventId = UUID.randomUUID().toString(),
            eventType = StreamingEventType.MESSAGE_SENT,
            tenantId = tenantId,
            queueName = queueName,
            data = StreamingMessage(
                messageId = messageId,
                tenantId = tenantId,
                queueName = queueName,
                messageBody = messageBody,
                attributes = attributes
            )
        )
        return publishEvent(event)
    }

    /**
     * 메시지 수신 이벤트를 발송합니다
     */
    fun publishMessageReceived(tenantId: String, queueName: String, messageId: String, receiptHandle: String): Mono<Void> {
        val event = StreamingEvent(
            eventId = UUID.randomUUID().toString(),
            eventType = StreamingEventType.MESSAGE_RECEIVED,
            tenantId = tenantId,
            queueName = queueName,
            data = mapOf(
                "messageId" to messageId,
                "receiptHandle" to receiptHandle,
                "timestamp" to Instant.now()
            )
        )
        return publishEvent(event)
    }

    /**
     * 메시지 삭제 이벤트를 발송합니다
     */
    fun publishMessageDeleted(tenantId: String, queueName: String, messageId: String): Mono<Void> {
        val event = StreamingEvent(
            eventId = UUID.randomUUID().toString(),
            eventType = StreamingEventType.MESSAGE_DELETED,
            tenantId = tenantId,
            queueName = queueName,
            data = mapOf(
                "messageId" to messageId,
                "timestamp" to Instant.now()
            )
        )
        return publishEvent(event)
    }

    /**
     * 특정 연결에 대한 이벤트 스트림을 가져옵니다
     */
    fun getEventStream(connectionId: String): Flux<StreamingEvent> {
        val sink = eventSinks[connectionId] 
            ?: return Flux.error(IllegalArgumentException("연결을 찾을 수 없습니다: $connectionId"))

        val connection = connections[connectionId] 
            ?: return Flux.error(IllegalArgumentException("연결을 찾을 수 없습니다: $connectionId"))

        return sink.asFlux()
            .filter { event -> shouldDeliverEvent(connection, event) }
            .doOnNext { event ->
                logger.debug("Event delivered - connectionId: $connectionId, eventType: ${event.eventType}, queue: ${event.queueName}")
            }
    }

    /**
     * 하트비트 이벤트를 생성합니다
     */
    fun sendHeartbeat(): Flux<Void> {
        return Flux.interval(Duration.ofSeconds(30))
            .flatMap {
                val now = Instant.now()
                val heartbeatEvents = connections.values.map { connection ->
                    connections[connection.connectionId] = connection.copy(lastHeartbeat = now)
                    
                    StreamingEvent(
                        eventId = UUID.randomUUID().toString(),
                        eventType = StreamingEventType.HEARTBEAT,
                        tenantId = connection.tenantId,
                        queueName = "*",
                        data = mapOf(
                            "connectionId" to connection.connectionId,
                            "timestamp" to now
                        )
                    )
                }
                
                Flux.fromIterable(heartbeatEvents)
                    .flatMap { event -> publishEvent(event) }
            }
    }

    /**
     * 연결 상태를 확인하고 비활성 연결을 정리합니다
     */
    fun cleanupInactiveConnections(): Flux<Void> {
        return Flux.interval(Duration.ofMinutes(5))
            .flatMap {
                val now = Instant.now()
                val inactiveThreshold = now.minusSeconds(300) // 5분

                val inactiveConnections = connections.values
                    .filter { it.lastHeartbeat.isBefore(inactiveThreshold) }

                Flux.fromIterable(inactiveConnections)
                    .flatMap { connection ->
                        logger.warn("Cleaning up inactive connection - connectionId: ${connection.connectionId}")
                        closeConnection(connection.connectionId)
                    }
            }
    }

    /**
     * 활성 연결 수를 반환합니다
     */
    fun getActiveConnectionCount(): Int = connections.size

    /**
     * 특정 테넌트의 활성 연결 수를 반환합니다
     */
    fun getActiveConnectionCount(tenantId: String): Int = 
        connections.values.count { it.tenantId == tenantId }

    // ========== 내부 헬퍼 메서드들 ==========

    private fun publishEvent(event: StreamingEvent): Mono<Void> {
        return Mono.fromRunnable<Void> {
            eventSinks.values.forEach { sink ->
                sink.tryEmitNext(event)
            }
        }
    }

    private fun shouldDeliverEvent(connection: StreamingConnection, event: StreamingEvent): Boolean {
        // 테넌트 격리 확인
        if (connection.tenantId != event.tenantId) {
            return false
        }

        // 하트비트는 항상 전달
        if (event.eventType == StreamingEventType.HEARTBEAT) {
            return true
        }

        // 연결 관련 이벤트는 해당 연결에만 전달
        if (event.eventType in setOf(StreamingEventType.CONNECTION_ESTABLISHED, StreamingEventType.CONNECTION_LOST)) {
            val eventConnectionId = (event.data as? Map<*, *>)?.get("connectionId") as? String
            return eventConnectionId == connection.connectionId
        }

        // 구독 기반 필터링
        return connection.subscriptions.any { subscription ->
            // 이벤트 타입 확인
            subscription.eventTypes.contains(event.eventType) &&
            // 큐 이름 확인 (빈 셋이면 모든 큐)
            (subscription.queueNames.isEmpty() || subscription.queueNames.contains(event.queueName)) &&
            // 필터 표현식 확인 (현재는 구현하지 않음)
            (subscription.filterExpression == null)
        }
    }
}