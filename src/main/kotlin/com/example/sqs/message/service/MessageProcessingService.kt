package com.example.sqs.message.service

import com.example.sqs.common.service.BaseReactiveService
import com.example.sqs.config.QueueProperties
import com.example.sqs.performance.compression.MessageCompressionService
import com.example.sqs.performance.partitioning.QueuePartitioningService
import org.springframework.stereotype.Service
import kotlin.math.abs

/**
 * 메시지 처리 유틸리티 기능을 담당하는 서비스
 */
@Service
class MessageProcessingService(
    private val compressionService: MessageCompressionService,
    private val partitioningService: QueuePartitioningService,
    private val queueProperties: QueueProperties
) : BaseReactiveService() {

    override fun getServiceName(): String = "MessageProcessingService"

    /**
     * 메시지 압축 처리
     */
    fun processMessageCompression(messageBody: String): Pair<String, Boolean> {
        return if (queueProperties.enableCompression && compressionService.shouldCompress(messageBody, queueProperties.compressionThreshold)) {
            val compressed = compressionService.compress(messageBody)
            compressed.toString(Charsets.ISO_8859_1) to true
        } else {
            messageBody to false
        }
    }

    /**
     * 메시지 압축 해제
     */
    fun decompressMessage(messageBody: String, attributes: Map<String, String>): String {
        return if (attributes["_compressed"] == "true") {
            val compressedData = messageBody.toByteArray(Charsets.ISO_8859_1)
            compressionService.decompress(compressedData)
        } else {
            messageBody
        }
    }

    /**
     * 메시지 파티션 결정
     */
    fun getPartitionForMessage(messageBody: String): Int {
        return if (queueProperties.enablePartitioning) {
            partitioningService.getPartitionNumber(messageBody, queueProperties.partitionCount)
        } else {
            0
        }
    }

    /**
     * FIFO 큐에 대한 파티션 번호 계산
     */
    fun calculateFifoPartition(messageGroupId: String, partitionCount: Int = 4): Int {
        return abs(messageGroupId.hashCode()) % partitionCount
    }

    /**
     * 표준 큐에 대한 파티션 번호 계산
     */
    fun calculateStandardPartition(messageContent: String, partitionCount: Int = 4): Int {
        return abs(messageContent.hashCode()) % partitionCount
    }

    /**
     * 메시지 속성에 메타데이터 추가
     */
    fun enhanceMessageAttributes(
        originalAttributes: Map<String, String>?,
        compressed: Boolean,
        originalSize: Int? = null,
        partition: String? = null
    ): MutableMap<String, String> {
        val enhanced = (originalAttributes ?: emptyMap()).toMutableMap()

        if (compressed) {
            enhanced["_compressed"] = "true"
            originalSize?.let { enhanced["_original_size"] = it.toString() }
        }

        partition?.let { enhanced["_partition"] = it }

        return enhanced
    }
}
