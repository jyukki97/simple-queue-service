// 관리자 대시보드 타입 정의

export interface SystemStats {
  totalTenants: number;
  totalQueues: number;
  totalMessages: number;
  totalMessagesProcessed: number;
  systemUptime: number;
  memoryUsage: MemoryUsage;
  redisStats: RedisStats;
}

export interface MemoryUsage {
  usedMemory: number;
  maxMemory: number;
  usagePercentage: number;
}

export interface RedisStats {
  connectedClients: number;
  usedMemory: number;
  keyCount: number;
  opsPerSecond: number;
}

export interface TenantUsageStats {
  tenantId: string;
  tenantName: string;
  queueCount: number;
  messageCount: number;
  messagesProcessedToday: number;
  storageUsed: number;
  apiCallsToday: number;
  lastActivity: string;
  isActive: boolean;
}

export interface ActivityLog {
  id: string;
  tenantId?: string;
  action: string;
  description: string;
  timestamp: string;
  severity: ActivitySeverity;
}

export enum ActivitySeverity {
  INFO = 'INFO',
  WARNING = 'WARNING',
  ERROR = 'ERROR'
}

export interface SystemAlert {
  id: string;
  type: AlertType;
  title: string;
  message: string;
  severity: AlertSeverity;
  timestamp: string;
  isResolved: boolean;
}

export enum AlertType {
  MEMORY_HIGH = 'MEMORY_HIGH',
  DISK_FULL = 'DISK_FULL',
  QUEUE_OVERFLOW = 'QUEUE_OVERFLOW',
  DLQ_THRESHOLD = 'DLQ_THRESHOLD',
  TENANT_QUOTA_EXCEEDED = 'TENANT_QUOTA_EXCEEDED'
}

export enum AlertSeverity {
  LOW = 'LOW',
  MEDIUM = 'MEDIUM',
  HIGH = 'HIGH',
  CRITICAL = 'CRITICAL'
}

export interface AdminDashboard {
  systemStats: SystemStats;
  tenantUsageStats: TenantUsageStats[];
  recentActivities: ActivityLog[];
  systemAlerts: SystemAlert[];
}

export interface QueueStats {
  [tenantId: string]: {
    [queueName: string]: {
      messagesAvailable: number;
      messagesInFlight: number;
      messagesDelayed?: number;
      oldestMessageAge?: number;
    };
  };
}

// API Response 타입
export interface ApiResponse<T> {
  success: boolean;
  data?: T;
  error?: {
    code: string;
    message: string;
  };
  timestamp: string;
}

// 스토리지 관련 타입
export interface StorageStats {
  HOT: TierMetrics;
  WARM: TierMetrics;
  COLD: TierMetrics;
}

export interface TierMetrics {
  tier: StorageTier;
  messageCount: number;
  storageUsageBytes: number;
  avgAccessTimeMs: number;
  hitRate: number;
  migrationRate: number;
  errorRate: number;
  lastUpdated: string;
}

export enum StorageTier {
  HOT = 'HOT',
  WARM = 'WARM',
  COLD = 'COLD'
}

// 대시보드 전용 타입
export interface DashboardData {
  totalTenants: number;
  totalQueues: number;
  totalMessages: number;
  processedMessages: number;
  storageUsage: {
    hot: StorageUsage;
    warm: StorageUsage;
    cold: StorageUsage;
  };
  messageStats: MessageStat[];
  topQueues: QueueMetric[];
  recentAlerts?: Alert[];
}

export interface StorageUsage {
  used: number;
  total: number;
  percentage: number;
}

export interface MessageStat {
  timestamp: string;
  sent: number;
  received: number;
  processed: number;
}

export interface QueueMetric {
  queueName: string;
  messageCount: number;
  processingRate: number;
  tenantId: string;
}

export interface Alert {
  id: string;
  title: string;
  message: string;
  severity: 'info' | 'warning' | 'critical';
  timestamp: string;
}

export interface SystemStatus {
  overall: 'healthy' | 'warning' | 'critical';
  components: {
    redis: string;
    storage: string;
    application: string;
  };
  uptime: string;
  lastCheck: string;
}

export interface QueueMetrics {
  queueName: string;
  messageCount: number;
  processingRate: number;
}

export interface SystemMetrics {
  timestamp: number;
  totalQueues: number;
  totalMessages: number;
  totalTenants: number;
  totalThroughputPerSecond: number;
  averageLatency: number;
  errorRate: number;
  memoryUsageBytes: number;
  cpuUsagePercent: number;
  redisConnectionsActive: number;
  redisConnectionsIdle: number;
}

export interface SystemMetricPoint {
  timestamp: string;
  cpuUsage: number;
  memoryUsage: number;
  diskUsage: number;
  networkIn: number;
  networkOut: number;
  activeConnections: number;
  queueOperations: number;
  messageThroughput: number;
}

export interface PerformanceAlert {
  id: string;
  type: 'cpu' | 'memory' | 'disk' | 'network' | 'queue';
  message: string;
  severity: 'warning' | 'critical';
  timestamp: string;
  value: number;
  threshold: number;
}
