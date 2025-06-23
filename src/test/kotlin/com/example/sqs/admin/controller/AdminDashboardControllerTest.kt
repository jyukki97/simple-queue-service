package com.example.sqs.admin.controller

import com.example.sqs.admin.domain.*
import com.example.sqs.admin.service.AdminDashboardService
import com.example.sqs.admin.service.SystemAlertService
import com.example.sqs.admin.service.ActivityLogService
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mock
import org.mockito.Mockito.`when`
import org.mockito.junit.jupiter.MockitoExtension
import org.springframework.test.context.junit.jupiter.SpringExtension
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@ExtendWith(MockitoExtension::class, SpringExtension::class)
class AdminDashboardControllerTest {

    @Mock
    private lateinit var adminDashboardService: AdminDashboardService
    
    @Mock
    private lateinit var alertService: SystemAlertService
    
    @Mock
    private lateinit var activityLogService: ActivityLogService
    
    private lateinit var adminDashboardController: AdminDashboardController

    // 테스트 데이터
    private val mockDashboard = AdminDashboard(
        systemStats = SystemStats(
            totalTenants = 10,
            totalQueues = 50,
            totalMessages = 1000,
            totalMessagesProcessed = 900,
            systemUptime = 86400,
            memoryUsage = MemoryUsage(
                usedMemory = 512_000_000,
                maxMemory = 1_000_000_000,
                usagePercentage = 51.2
            ),
            redisStats = RedisStats(
                connectedClients = 5,
                usedMemory = 100_000_000,
                keyCount = 10000,
                opsPerSecond = 200.0
            )
        ),
        tenantUsageStats = listOf(),
        recentActivities = listOf(),
        systemAlerts = listOf()
    )
    
    private val mockQueueStats = mapOf(
        "tenant-1" to mapOf(
            "queue-1" to mapOf(
                "messagesAvailable" to 100,
                "messagesInFlight" to 20
            )
        )
    )
    
    private val mockAlerts = listOf(
        SystemAlert(
            id = "alert-1",
            type = AlertType.MEMORY_HIGH,
            title = "High Memory Usage",
            message = "Memory usage exceeds 80%",
            severity = AlertSeverity.HIGH,
            timestamp = Instant.now(),
            isResolved = false
        )
    )
    
    private val mockActivities = listOf(
        ActivityLog(
            id = "activity-1",
            tenantId = "tenant-1",
            action = "CREATE_QUEUE",
            description = "큐 생성됨",
            timestamp = Instant.now(),
            severity = ActivitySeverity.INFO
        )
    )

    @BeforeEach
    fun setup() {
        adminDashboardController = AdminDashboardController(
            adminDashboardService, 
            alertService,
            activityLogService
        )
    }

    @Test
    fun `대시보드 조회시 정상 응답 반환`() = runBlocking {
        // Given
        `when`(adminDashboardService.getDashboard()).thenReturn(mockDashboard)
        
        // When
        val response = adminDashboardController.getDashboard()
        
        // Then
        assertTrue { response.success }
        assertNotNull(response.data)
        assertEquals(10L, response.data?.systemStats?.totalTenants)
        assertEquals(50L, response.data?.systemStats?.totalQueues)
    }
    
    @Test
    fun `큐 통계 조회시 정상 응답 반환`() = runBlocking {
        // Given
        `when`(adminDashboardService.getQueueStats()).thenReturn(mockQueueStats)
        
        // When
        val response = adminDashboardController.getQueueStats()
        
        // Then
        assertTrue { response.success }
        assertNotNull(response.data)
        assertTrue { response.data!!.containsKey("tenant-1") }
        
        val tenantQueues = response.data!!["tenant-1"]
        assertNotNull(tenantQueues)
        assertTrue { tenantQueues!!.containsKey("queue-1") }
    }
    
    @Test
    fun `경고 목록 조회시 정상 응답 반환`() {
        // Given
        `when`(alertService.getActiveAlerts()).thenReturn(mockAlerts)
        
        // When
        val response = adminDashboardController.getAlerts()
        
        // Then
        assertTrue { response.success }
        assertNotNull(response.data)
        assertEquals(1, response.data!!.size)
        assertEquals("alert-1", response.data!![0].id)
        assertEquals(AlertType.MEMORY_HIGH, response.data!![0].type)
    }
    
    @Test
    fun `특정 경고 조회시 정상 응답 반환`() {
        // Given
        `when`(alertService.getAlert("alert-1")).thenReturn(mockAlerts[0])
        
        // When
        val response = adminDashboardController.getAlert("alert-1")
        
        // Then
        assertTrue { response.success }
        assertNotNull(response.data)
        assertEquals("alert-1", response.data!!.id)
    }
    
    @Test
    fun `경고 해결 처리시 정상 응답 반환`() {
        // Given
        `when`(alertService.resolveAlert("alert-1")).thenReturn(true)
        
        // When
        val response = adminDashboardController.resolveAlert("alert-1")
        
        // Then
        assertTrue { response.success }
        assertNotNull(response.data)
        assertTrue { response.data!! }
    }
    
    @Test
    fun `활동 로그 조회시 정상 응답 반환`() {
        // Given
        `when`(activityLogService.getRecentActivities(20)).thenReturn(mockActivities)
        
        // When
        val response = adminDashboardController.getActivities(20)
        
        // Then
        assertTrue { response.success }
        assertNotNull(response.data)
        assertEquals(1, response.data!!.size)
        assertEquals("activity-1", response.data!![0].id)
    }
}
