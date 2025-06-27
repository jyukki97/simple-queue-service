package com.example.sqs.storage.domain.cold

import java.time.Instant

/**
 * 파일 메타데이터 (Phase 2: 무결성 강화)
 */
data class FileMetadata(
    val fileName: String,
    val messageCount: Int,
    val totalSizeBytes: Long,
    val compressionRatio: Double,
    val createdAt: Instant,
    val lastModifiedAt: Instant,
    val messageIdRange: Pair<String, String>?,
    val timestampRange: Pair<Instant, Instant>?,
    val checksum: String,
    val checksumAlgorithm: String = "SHA-256",
    val integrityVerified: Boolean = false,
    val lastVerifiedAt: Instant? = null
)
