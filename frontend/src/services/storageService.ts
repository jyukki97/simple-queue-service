import axios, { AxiosResponse } from 'axios';

const API_BASE_URL = process.env.REACT_APP_API_URL || '';

export interface TierStats {
  tier: string;
  messageCount: number;
  oldestMessageTime?: string;
  newestMessageTime?: string;
  totalSizeBytes: number;
}

export interface QueueStorageStats {
  queueName: string;
  queueType: string;
  hotStats: TierStats;
  warmStats: TierStats;
  coldStats: TierStats;
}

export interface StorageStatsResponse {
  totalQueues: number;
  queueStats: QueueStorageStats[];
  aggregatedStats: {
    totalMessages: number;
    totalHotMessages: number;
    totalWarmMessages: number;
    totalColdMessages: number;
    totalStorageBytes: number;
  };
}

export interface TierMessageInfo {
  messageId: string;
  messageBody: string;
  attributes: Record<string, string>;
  sentTimestamp: string;
  receivedCount: number;
  tier: string;
  movedToTierAt?: string;
  sizeBytes: number;
  compressionType?: string;
}

export interface TierMessagesResponse {
  queueName: string;
  tier: string;
  messages: TierMessageInfo[];
  totalCount: number;
  offset: number;
  maxMessages: number;
}

export interface TierMessagesSummary {
  tier: string;
  messageCount: number;
  sampleMessages: TierMessageInfo[];
  totalSizeBytes: number;
}

export interface StorageMessagesSummaryResponse {
  queueName: string;
  hotMessages: TierMessagesSummary;
  warmMessages: TierMessagesSummary;
  coldMessages: TierMessagesSummary;
}

export interface MessageLocationResponse {
  messageId: string;
  currentTier: string;
  lastMovedAt: string;
  createdAt: string;
}

export interface PromoteMessageRequest {
  targetTier: string;
  reason?: string;
}

export interface PromoteMessageResponse {
  messageId: string;
  fromTier: string;
  toTier: string;
  success: boolean;
  reason: string;
}

export interface ApiResponse<T> {
  success: boolean;
  data?: T;
  error?: {
    code: string;
    message: string;
  };
  timestamp: string;
}

class StorageService {
  private apiKey = 'test-api-key'; // TODO: 실제 환경에서는 안전한 방식으로 관리

  private getHeaders() {
    return {
      'X-API-Key': this.apiKey,
      'Content-Type': 'application/json'
    };
  }

  /**
   * 전체 스토리지 통계 조회
   */
  async getStorageStats(): Promise<StorageStatsResponse> {
    const response: AxiosResponse<ApiResponse<StorageStatsResponse>> = await axios.get(
      `${API_BASE_URL}/api/v1/storage/stats`,
      { headers: this.getHeaders() }
    );
    
    if (!response.data.success) {
      throw new Error(response.data.error?.message || 'Failed to fetch storage stats');
    }
    
    return response.data.data!;
  }

  /**
   * 특정 큐의 스토리지 통계 조회
   */
  async getQueueStorageStats(queueName: string): Promise<QueueStorageStats> {
    const response: AxiosResponse<ApiResponse<QueueStorageStats>> = await axios.get(
      `${API_BASE_URL}/api/v1/storage/queues/${encodeURIComponent(queueName)}/stats`,
      { headers: this.getHeaders() }
    );
    
    if (!response.data.success) {
      throw new Error(response.data.error?.message || 'Failed to fetch queue storage stats');
    }
    
    return response.data.data!;
  }

  /**
   * 특정 스토리지 계층의 메시지 목록 조회
   */
  async getMessagesByTier(
    queueName: string, 
    tier: string, 
    maxMessages: number = 10, 
    offset: number = 0
  ): Promise<TierMessagesResponse> {
    const params = new URLSearchParams({
      maxMessages: maxMessages.toString(),
      offset: offset.toString()
    });

    const response: AxiosResponse<TierMessagesResponse> = await axios.get(
      `${API_BASE_URL}/api/v1/storage/queues/${encodeURIComponent(queueName)}/tiers/${tier}/messages?${params.toString()}`,
      { headers: this.getHeaders() }
    );
    
    return response.data;
  }

  /**
   * 스토리지 계층별 메시지 요약 조회
   */
  async getMessagesSummaryByTier(queueName: string): Promise<StorageMessagesSummaryResponse> {
    const response: AxiosResponse<StorageMessagesSummaryResponse> = await axios.get(
      `${API_BASE_URL}/api/v1/storage/queues/${encodeURIComponent(queueName)}/messages/by-tier`,
      { headers: this.getHeaders() }
    );
    
    return response.data;
  }

  /**
   * 메시지 위치 정보 조회
   */
  async getMessageLocation(messageId: string): Promise<MessageLocationResponse> {
    const response: AxiosResponse<MessageLocationResponse> = await axios.get(
      `${API_BASE_URL}/api/v1/storage/messages/${encodeURIComponent(messageId)}/location`,
      { headers: this.getHeaders() }
    );
    
    return response.data;
  }

  /**
   * 메시지 수동 승격
   */
  async promoteMessage(messageId: string, request: PromoteMessageRequest): Promise<PromoteMessageResponse> {
    const response: AxiosResponse<PromoteMessageResponse> = await axios.post(
      `${API_BASE_URL}/api/v1/storage/messages/${encodeURIComponent(messageId)}/promote`,
      request,
      { headers: this.getHeaders() }
    );
    
    return response.data;
  }

  /**
   * 스토리지 계층 이름을 한국어로 변환
   */
  getTierDisplayName(tier: string): string {
    switch (tier.toUpperCase()) {
      case 'HOT':
        return '핫 스토리지';
      case 'WARM':
        return '웜 스토리지';
      case 'COLD':
        return '콜드 스토리지';
      default:
        return tier;
    }
  }

  /**
   * 스토리지 계층별 색상 반환
   */
  getTierColor(tier: string): string {
    switch (tier.toUpperCase()) {
      case 'HOT':
        return '#ff4d4f';
      case 'WARM':
        return '#faad14';
      case 'COLD':
        return '#1890ff';
      default:
        return '#666666';
    }
  }

  /**
   * 바이트 크기를 읽기 쉬운 형태로 변환
   */
  formatBytes(bytes: number): string {
    if (bytes === 0) return '0 Bytes';
    
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }
}

const storageService = new StorageService();
export default storageService;
