package com.example.sqs.storage.controller

import com.example.sqs.common.domain.ApiResponse
import com.example.sqs.message.domain.Message
import com.example.sqs.storage.domain.WarmStorageStats
import com.example.sqs.storage.dto.BatchMoveRequest
import com.example.sqs.storage.dto.BatchMoveResult
import com.example.sqs.storage.service.WarmTierManagerService
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono


/**
 * WARM 계층 관리 API 컨트롤러
 */
@RestController
@RequestMapping("/api/v1/storage/warm")
class WarmStorageController(
    private val warmTierManager: WarmTierManagerService
) {

    /**
     * 특정 메시지를 WARM 계층으로 이동
     */
    @PostMapping("/messages/{messageId}/move")
    fun moveMessageToWarm(
        @PathVariable messageId: String,
        @RequestParam tenantId: String,
        @RequestParam queueName: String
    ): Mono<ApiResponse<Boolean>> {
        return warmTierManager.moveMessageToWarm(messageId, tenantId, queueName)
            .map { success ->
                if (success) {
                    ApiResponse.success(true, "메시지가 WARM 계층으로 이동되었습니다")
                } else {
                    ApiResponse.error("WARM_MOVE_FAILED", "메시지 WARM 계층 이동에 실패했습니다")
                }
            }
    }

    /**
     * 배치로 메시지들을 WARM 계층으로 이동
     */
    @PostMapping("/messages/batch-move")
    fun moveMessagesToWarmBatch(
        @RequestBody request: BatchMoveRequest
    ): Mono<ApiResponse<BatchMoveResult>> {
        return warmTierManager.moveMessagesToWarmBatch(
            request.messageIds,
            request.tenantId,
            request.queueName
        ).map { result ->
            ApiResponse.success(result, "배치 이동이 완료되었습니다")
        }
    }

    /**
     * WARM 계층에서 메시지 조회
     */
    @GetMapping("/messages/{messageId}")
    fun getMessageFromWarm(
        @PathVariable messageId: String
    ): Mono<ApiResponse<Message?>> {
        return warmTierManager.getMessageFromWarm(messageId)
            .map { message ->
                if (message != null) {
                    ApiResponse.success(message, "메시지 조회 성공")
                } else {
                    ApiResponse.error("MESSAGE_NOT_FOUND", "WARM 계층에서 메시지를 찾을 수 없습니다")
                }
            }
    }

    /**
     * WARM 계층에서 메시지 삭제
     */
    @DeleteMapping("/messages/{messageId}")
    fun deleteMessageFromWarm(
        @PathVariable messageId: String
    ): Mono<ApiResponse<Boolean>> {
        return warmTierManager.deleteMessageFromWarm(messageId)
            .map { deleted ->
                if (deleted) {
                    ApiResponse.success(true, "메시지가 삭제되었습니다")
                } else {
                    ApiResponse.error("DELETE_FAILED", "메시지 삭제에 실패했습니다")
                }
            }
    }

    /**
     * 큐별 WARM 메시지 목록 조회
     */
    @GetMapping("/queues/{queueName}/messages")
    fun getWarmMessagesByQueue(
        @PathVariable queueName: String,
        @RequestParam(defaultValue = "tenant1") tenantId: String,
        @RequestParam(defaultValue = "100") limit: Int,
        @RequestParam(defaultValue = "0") offset: Int
    ): Flux<Message> {
        return warmTierManager.getWarmMessagesByQueue(tenantId, queueName, limit, offset)
    }

    /**
     * WARM 계층 통계 조회
     */
    @GetMapping("/stats")
    fun getWarmStats(): Mono<ApiResponse<WarmStorageStats>> {
        return warmTierManager.getWarmStats()
            .map { stats ->
                ApiResponse.success(stats, "WARM 계층 통계 조회 성공")
            }
    }

    /**
     * 디버그: 모든 메시지 정보 조회
     */
    @GetMapping("/debug/all-messages")
    fun debugAllMessages(): Mono<ApiResponse<List<Map<String, Any>>>> {
        return warmTierManager.debugAllMessages()
            .map { messages ->
                ApiResponse.success(messages, "모든 메시지 정보 조회 성공")
            }
    }

    /**
     * 디버그: 처음 몇 개 메시지 정보 확인
     */
    @GetMapping("/debug/first-messages")
    fun debugFirstMessages(
        @RequestParam(defaultValue = "5") count: Int
    ): Mono<ApiResponse<List<Map<String, String>>>> {
        return warmTierManager.debugFirstFewMessages(count)
            .map { messages -> ApiResponse.success(messages) }
    }

    /**
     * 만료된 메시지 정리
     */
    @PostMapping("/cleanup")
    fun cleanupExpiredMessages(): Mono<ApiResponse<Int>> {
        return warmTierManager.cleanupExpiredMessages()
            .map { count ->
                ApiResponse.success(count, "$count 개의 만료된 메시지가 정리되었습니다")
            }
    }
}
