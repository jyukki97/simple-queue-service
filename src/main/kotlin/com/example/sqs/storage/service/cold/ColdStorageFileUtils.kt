package com.example.sqs.storage.service.cold

import com.example.sqs.message.domain.Message
import com.example.sqs.storage.domain.cold.FileMetadata
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.io.FileInputStream
import java.io.FileOutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

/**
 * 콜드 스토리지 파일 조작 유틸리티
 */
@Component
class ColdStorageFileUtils(
    private val objectMapper: ObjectMapper
) {
    private val logger = LoggerFactory.getLogger(ColdStorageFileUtils::class.java)

    companion object {
        const val MESSAGE_FILE_EXTENSION = ".json.gz"
        val DATE_FORMATTER: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd")
    }

    /**
     * 아카이브 경로 생성
     */
    fun createArchivePath(basePath: String, date: LocalDate): Path {
        val dateStr = date.format(DATE_FORMATTER)
        val archivePath = java.nio.file.Paths.get(basePath, "archives", dateStr)
        Files.createDirectories(archivePath)
        return archivePath
    }

    /**
     * 압축된 파일 쓰기
     */
    fun writeCompressedFile(file: Path, content: String, compressionLevel: Int = 6) {
        Files.createDirectories(file.parent)
        FileOutputStream(file.toFile()).use { fos ->
            GZIPOutputStream(fos).use { gzos ->
                gzos.write(content.toByteArray(Charsets.UTF_8))
            }
        }
    }

    /**
     * 압축된 파일에서 메시지 읽기
     */
    fun readMessagesFromFile(file: Path): List<Message> {
        if (!Files.exists(file)) return emptyList()

        return try {
            FileInputStream(file.toFile()).use { fis ->
                GZIPInputStream(fis).use { gzis ->
                    val json = gzis.readBytes().toString(Charsets.UTF_8)
                    objectMapper.readValue(json, objectMapper.typeFactory.constructCollectionType(List::class.java, Message::class.java))
                }
            }
        } catch (e: Exception) {
            logger.error("파일 읽기 실패: $file", e)
            emptyList()
        }
    }

    /**
     * 파일의 메시지 수 계산
     */
    fun countMessagesInFile(file: Path): Int {
        return try {
            readMessagesFromFile(file).size
        } catch (e: Exception) {
            0
        }
    }

    /**
     * 메타데이터 저장
     */
    fun saveMetadata(filePath: Path, metadata: FileMetadata) {
        try {
            val metadataFile = filePath.parent.resolve("${filePath.fileName}.metadata.json")
            val json = objectMapper.writeValueAsString(metadata)
            Files.write(metadataFile, json.toByteArray(Charsets.UTF_8))
        } catch (e: Exception) {
            logger.error("메타데이터 저장 실패: $filePath", e)
        }
    }

    /**
     * 메타데이터 읽기
     */
    fun readMetadata(directory: Path, fileName: String): FileMetadata? {
        return try {
            val metadataFile = directory.resolve("${fileName}.metadata.json")
            if (Files.exists(metadataFile)) {
                val json = Files.readString(metadataFile, Charsets.UTF_8)
                objectMapper.readValue(json, FileMetadata::class.java)
            } else {
                null
            }
        } catch (e: Exception) {
            logger.error("메타데이터 읽기 실패: $directory/$fileName", e)
            null
        }
    }

    /**
     * 특정 메시지를 파일들에서 검색
     */
    fun searchMessageAcrossFiles(basePath: String, messageId: String): Message? {
        val currentDate = LocalDate.now()
        var searchDate = currentDate

        // 최근 30일간 검색
        repeat(30) {
            val archivePath = createArchivePath(basePath, searchDate)
            if (Files.exists(archivePath)) {
                Files.list(archivePath).use { stream ->
                    val files = stream.filter { it.toString().endsWith(MESSAGE_FILE_EXTENSION) }.toList()
                    for (file in files) {
                        val messages = readMessagesFromFile(file)
                        val found = messages.find { it.messageId == messageId }
                        if (found != null) {
                            return found
                        }
                    }
                }
            }
            searchDate = searchDate.minusDays(1)
        }

        return null
    }
}
