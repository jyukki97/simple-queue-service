package com.example.sqs.common.validation

import com.example.sqs.common.exception.ValidationException

/**
 * 도메인 객체 검증을 위한 공통 인터페이스
 */
interface Validator<T> {
    /**
     * 객체를 검증합니다
     * @param obj 검증할 객체
     * @throws ValidationException 검증 실패 시
     */
    fun validate(obj: T)
    
    /**
     * 객체를 검증하고 결과를 반환합니다
     * @param obj 검증할 객체
     * @return 검증 결과 (성공: true, 실패: false)
     */
    fun isValid(obj: T): Boolean {
        return try {
            validate(obj)
            true
        } catch (e: ValidationException) {
            false
        }
    }
}

/**
 * 기본 검증 규칙들을 제공하는 유틸리티 클래스
 */
object ValidationRules {
    
    fun notNull(value: Any?, fieldName: String) {
        if (value == null) {
            throw ValidationException("$fieldName cannot be null")
        }
    }
    
    fun notBlank(value: String?, fieldName: String) {
        if (value.isNullOrBlank()) {
            throw ValidationException("$fieldName cannot be blank")
        }
    }
    
    fun minLength(value: String?, minLength: Int, fieldName: String) {
        if (value != null && value.length < minLength) {
            throw ValidationException("$fieldName must be at least $minLength characters")
        }
    }
    
    fun maxLength(value: String?, maxLength: Int, fieldName: String) {
        if (value != null && value.length > maxLength) {
            throw ValidationException("$fieldName must not exceed $maxLength characters")
        }
    }
    
    fun positive(value: Number?, fieldName: String) {
        if (value != null && value.toDouble() <= 0) {
            throw ValidationException("$fieldName must be positive")
        }
    }
    
    fun nonNegative(value: Number?, fieldName: String) {
        if (value != null && value.toDouble() < 0) {
            throw ValidationException("$fieldName must be non-negative")
        }
    }
    
    fun inRange(value: Number?, min: Number, max: Number, fieldName: String) {
        if (value != null) {
            val doubleValue = value.toDouble()
            if (doubleValue < min.toDouble() || doubleValue > max.toDouble()) {
                throw ValidationException("$fieldName must be between $min and $max")
            }
        }
    }
}
