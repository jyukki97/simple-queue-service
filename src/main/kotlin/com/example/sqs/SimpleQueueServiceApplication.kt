package com.example.sqs

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class SimpleQueueServiceApplication

fun main(args: Array<String>) {
    runApplication<SimpleQueueServiceApplication>(*args)
}
