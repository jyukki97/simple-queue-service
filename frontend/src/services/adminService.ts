import axios, { AxiosResponse } from 'axios';
import { 
  QueueStats, 
  SystemAlert, 
  ActivityLog, 
  ApiResponse,
  StorageStats,
  DashboardData,
  SystemStatus,
  SystemStats,
  AlertSeverity
} from '../types/admin';

const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:8080';

class AdminService {
  private apiKey = 'test-api-key'; // TODO: 실제 환경에서는 안전한 방식으로 관리

  private getHeaders() {
    return {
      'X-API-Key': this.apiKey,
      'Content-Type': 'application/json'
    };
  }

  /**
   * 관리자 대시보드 데이터 조회
   */
  async getDashboard(): Promise<DashboardData> {
    // 여러 API 호출을 통해 대시보드 데이터 조합
    const [systemStats, storageStats, alerts] = await Promise.all([
      this.getSystemStats(),
      this.getStorageStats(),
      this.getAlerts()
    ]);

    // Mock data for demo - 실제로는 서버에서 가져와야 함
    const dashboardData: DashboardData = {
      totalTenants: systemStats.totalTenants,
      totalQueues: systemStats.totalQueues,
      totalMessages: systemStats.totalMessages,
      processedMessages: systemStats.totalMessagesProcessed,
      storageUsage: {
        hot: {
          used: storageStats.HOT.storageUsageBytes,
          total: 1024 * 1024 * 1024, // 1GB
          percentage: (storageStats.HOT.storageUsageBytes / (1024 * 1024 * 1024)) * 100
        },
        warm: {
          used: storageStats.WARM.storageUsageBytes,
          total: 10 * 1024 * 1024 * 1024, // 10GB
          percentage: (storageStats.WARM.storageUsageBytes / (10 * 1024 * 1024 * 1024)) * 100
        },
        cold: {
          used: storageStats.COLD.storageUsageBytes,
          total: 100 * 1024 * 1024 * 1024, // 100GB
          percentage: (storageStats.COLD.storageUsageBytes / (100 * 1024 * 1024 * 1024)) * 100
        }
      },
      messageStats: this.generateMockMessageStats(),
      topQueues: this.generateMockQueueMetrics(),
      recentAlerts: alerts.slice(0, 5).map((alert: SystemAlert) => ({
        id: alert.id,
        title: alert.title,
        message: alert.message,
        severity: this.mapAlertSeverity(alert.severity),
        timestamp: alert.timestamp
      }))
    };

    return dashboardData;
  }

  private async getSystemStats(): Promise<SystemStats> {
    const response: AxiosResponse<ApiResponse<SystemStats>> = await axios.get(
      `${API_BASE_URL}/api/v1/admin/dashboard/stats`,
      { headers: this.getHeaders() }
    );
    
    if (!response.data.success) {
      throw new Error(response.data.error?.message || 'Failed to fetch system stats');
    }
    
    return response.data.data!;
  }

  private generateMockMessageStats() {
    const stats = [];
    const now = new Date();
    for (let i = 23; i >= 0; i--) {
      const timestamp = new Date(now.getTime() - i * 60 * 60 * 1000);
      stats.push({
        timestamp: timestamp.toLocaleTimeString('ko-KR', { hour: '2-digit', minute: '2-digit' }),
        sent: Math.floor(Math.random() * 1000) + 500,
        received: Math.floor(Math.random() * 950) + 450,
        processed: Math.floor(Math.random() * 900) + 400
      });
    }
    return stats;
  }

  private generateMockQueueMetrics() {
    return [
      { queueName: 'orders-queue', messageCount: 1250, processingRate: 95.5, tenantId: 'tenant-1' },
      { queueName: 'notifications-queue', messageCount: 890, processingRate: 87.2, tenantId: 'tenant-2' },
      { queueName: 'analytics-queue', messageCount: 650, processingRate: 92.1, tenantId: 'tenant-1' },
      { queueName: 'emails-queue', messageCount: 420, processingRate: 89.8, tenantId: 'tenant-3' },
      { queueName: 'logs-queue', messageCount: 380, processingRate: 94.3, tenantId: 'tenant-2' }
    ];
  }

  private mapAlertSeverity(severity: AlertSeverity): 'info' | 'warning' | 'critical' {
    switch (severity) {
      case AlertSeverity.CRITICAL:
      case AlertSeverity.HIGH:
        return 'critical';
      case AlertSeverity.MEDIUM:
        return 'warning';
      default:
        return 'info';
    }
  }

  /**
   * 실시간 큐 통계 조회
   */
  async getQueueStats(): Promise<QueueStats> {
    const response: AxiosResponse<ApiResponse<QueueStats>> = await axios.get(
      `${API_BASE_URL}/api/v1/admin/dashboard/queues/stats`,
      { headers: this.getHeaders() }
    );
    
    if (!response.data.success) {
      throw new Error(response.data.error?.message || 'Failed to fetch queue stats');
    }
    
    return response.data.data!;
  }

  /**
   * 활성 알림 목록 조회
   */
  async getAlerts(): Promise<SystemAlert[]> {
    const response: AxiosResponse<ApiResponse<SystemAlert[]>> = await axios.get(
      `${API_BASE_URL}/api/v1/admin/dashboard/alerts`,
      { headers: this.getHeaders() }
    );
    
    if (!response.data.success) {
      throw new Error(response.data.error?.message || 'Failed to fetch alerts');
    }
    
    return response.data.data!;
  }

  /**
   * 알림 해결 처리
   */
  async resolveAlert(alertId: string): Promise<boolean> {
    const response: AxiosResponse<ApiResponse<boolean>> = await axios.post(
      `${API_BASE_URL}/api/v1/admin/dashboard/alerts/${alertId}/resolve`,
      {},
      { headers: this.getHeaders() }
    );
    
    if (!response.data.success) {
      throw new Error(response.data.error?.message || 'Failed to resolve alert');
    }
    
    return response.data.data!;
  }

  /**
   * 최근 활동 로그 조회
   */
  async getActivities(limit: number = 20): Promise<ActivityLog[]> {
    const response: AxiosResponse<ApiResponse<ActivityLog[]>> = await axios.get(
      `${API_BASE_URL}/api/v1/admin/dashboard/activities?limit=${limit}`,
      { headers: this.getHeaders() }
    );
    
    if (!response.data.success) {
      throw new Error(response.data.error?.message || 'Failed to fetch activities');
    }
    
    return response.data.data!;
  }

  /**
   * 스토리지 통계 조회
   */
  async getStorageStats(): Promise<StorageStats> {
    const response: AxiosResponse<ApiResponse<StorageStats>> = await axios.get(
      `${API_BASE_URL}/api/v1/storage/stats`,
      { headers: this.getHeaders() }
    );
    
    if (!response.data.success) {
      throw new Error(response.data.error?.message || 'Failed to fetch storage stats');
    }
    
    return response.data.data!;
  }

  /**
   * 자동 계층화 트리거
   */
  async triggerAutoTiering(): Promise<string> {
    const response: AxiosResponse<ApiResponse<string>> = await axios.post(
      `${API_BASE_URL}/api/v1/storage/trigger-auto-tiering`,
      {},
      { headers: this.getHeaders() }
    );
    
    if (!response.data.success) {
      throw new Error(response.data.error?.message || 'Failed to trigger auto-tiering');
    }
    
    return response.data.data!;
  }

  /**
   * 시스템 상태 조회
   */
  async getSystemStatus(): Promise<SystemStatus> {
    const response: AxiosResponse<ApiResponse<SystemStatus>> = await axios.get(
      `${API_BASE_URL}/api/v1/admin/system/status`,
      { headers: this.getHeaders() }
    );
    
    if (!response.data.success) {
      throw new Error(response.data.error?.message || 'Failed to fetch system status');
    }
    
    return response.data.data!;
  }
}

const adminService = new AdminService();
export default adminService;
