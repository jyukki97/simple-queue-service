package com.example.sqs.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.codec.ServerCodecConfigurer
import org.springframework.http.codec.json.Jackson2JsonDecoder
import org.springframework.http.codec.json.Jackson2JsonEncoder
import org.springframework.web.reactive.config.WebFluxConfigurer

@Configuration
class WebFluxConfig : WebFluxConfigurer {

    @Bean
    fun objectMapper(): ObjectMapper {
        return ObjectMapper()
            .registerKotlinModule()
            .registerModule(JavaTimeModule())
    }

    override fun configureHttpMessageCodecs(configurer: ServerCodecConfigurer) {
        val mapper = objectMapper()
        configurer.defaultCodecs().jackson2JsonEncoder(Jackson2JsonEncoder(mapper))
        configurer.defaultCodecs().jackson2JsonDecoder(Jackson2JsonDecoder(mapper))
    }
}
