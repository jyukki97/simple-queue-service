package com.example.sqs.performance.batch

import kotlinx.coroutines.reactor.awaitSingle
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

/**
 * Redis 배치 처리 최적화 서비스
 */
@Component
class RedisBatchService(
    private val redisTemplate: ReactiveRedisTemplate<String, Any>,
    private val stringRedisTemplate: ReactiveRedisTemplate<String, String>
) {
    
    /**
     * 여러 키를 한 번에 조회
     */
    suspend fun multiGet(keys: List<String>): List<String?> {
        return stringRedisTemplate.opsForValue()
            .multiGet(keys)
            .awaitSingle()
    }
    
    /**
     * 여러 키-값을 한 번에 설정
     */
    suspend fun multiSet(keyValues: Map<String, String>): Boolean {
        return stringRedisTemplate.opsForValue()
            .multiSet(keyValues)
            .awaitSingle()
    }
    
    /**
     * 여러 키를 한 번에 삭제
     */
    suspend fun multiDelete(keys: List<String>): Long {
        return stringRedisTemplate.delete(Flux.fromIterable(keys))
            .awaitSingle()
    }
    
    /**
     * 리스트에 여러 값을 한 번에 추가
     */
    suspend fun listPushAll(key: String, values: List<String>): Long {
        return stringRedisTemplate.opsForList()
            .leftPushAll(key, *values.toTypedArray())
            .awaitSingle()
    }
    
    /**
     * 정렬된 집합에 여러 값을 한 번에 추가
     */
    suspend fun zAddAll(key: String, scoreValuePairs: Map<String, Double>): Long {
        return Flux.fromIterable(scoreValuePairs.entries)
            .flatMap { (value, score) ->
                stringRedisTemplate.opsForZSet().add(key, value, score)
            }
            .reduce(0L) { acc, success -> 
                acc + if (success) 1L else 0L 
            }
            .awaitSingle()
    }
    
    /**
     * 배치 처리를 위한 파이프라인 실행
     */
    suspend fun executeBatch(operations: List<() -> Mono<Any>>): List<Any> {
        return Flux.fromIterable(operations)
            .flatMap { operation -> operation() }
            .collectList()
            .awaitSingle()
    }
}
