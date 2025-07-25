import axios, { AxiosResponse } from 'axios';

const API_BASE_URL = process.env.REACT_APP_API_URL || '';

export interface QueueAttributes {
  visibilityTimeoutSeconds: number;
  messageRetentionPeriod: number;
  delaySeconds: number;
  maxReceiveCount: number;
  deadLetterTargetArn?: string;
  redrivePolicy?: string;
  fifoQueue: boolean;
  contentBasedDeduplication?: boolean;
  receiveMessageWaitTimeSeconds?: number;
}

export interface Queue {
  queueName: string;
  tenantId: string;
  attributes: QueueAttributes;
  createdAt: number;
  updatedAt: number;
  messagesKey?: string;
  delayedKey?: string;
  inFlightKey?: string;
}

export interface QueueStatistics {
  queueName: string;
  approximateNumberOfMessages: number;
  approximateNumberOfMessagesNotVisible: number;
  approximateNumberOfMessagesInDLQ: number;
  approximateAgeOfOldestMessage?: number;
  lastUpdated: string;
}

export interface CreateQueueRequest {
  queueName: string;
  attributes?: Partial<QueueAttributes>;
}

export interface SendMessageRequest {
  messageBody: string;
  delaySeconds?: number;
  attributes?: Record<string, string>;
  messageGroupId?: string;  // FIFO 큐용
  messageDeduplicationId?: string;  // FIFO 큐용
}

export interface SendMessageBatchRequest {
  messages: Array<{
    id: string;
    messageBody: string;
    delaySeconds?: number;
    attributes?: Record<string, string>;
    messageGroupId?: string;  // FIFO 큐용
    messageDeduplicationId?: string;  // FIFO 큐용
  }>;
}

export interface SendMessageBatchResponse {
  successful: Array<{
    id: string;
    messageId: string;
    messageBodyMD5?: string;
  }>;
  failed: Array<{
    id: string;
    code: string;
    message: string;
    senderFault: boolean;
  }>;
}

export interface MessagePreview {
  messageId: string;
  messageBody: string;
  attributes: Record<string, string>;
  sentTimestamp: number;
  firstReceiveTimestamp?: number;
  receiveCount: number;
  approximateFirstReceiveTimestamp?: number;
  messageGroupId?: string;  // FIFO 큐용
  messageDeduplicationId?: string;  // FIFO 큐용
}

export interface PeekMessageResponse {
  messages: MessagePreview[];
}

export interface ReceiveMessageRequest {
  maxNumberOfMessages?: number;
  visibilityTimeoutSeconds?: number;
  waitTimeSeconds?: number;
  messageGroupId?: string;  // FIFO 큐용
}

export interface MessageResponse {
  messageId: string;
  messageBody: string;
  attributes: Record<string, string>;
  receiptHandle: string;
  sentTimestamp: number;
  messageGroupId?: string;  // FIFO 큐용
  messageDeduplicationId?: string;  // FIFO 큐용
}

export interface ReceiveMessageResponse {
  messages: MessageResponse[];
}

export interface SendMessageResponse {
  messageId: string;
  messageBodyMD5?: string;
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

class QueueService {
  private apiKey = 'test-api-key'; // Admin API key for testing

  private getHeaders() {
    return {
      'X-API-Key': this.apiKey,
      'Content-Type': 'application/json'
    };
  }

  /**
   * 큐 목록 조회
   */
  async getQueues(limit: number = 20, offset: number = 0, namePattern?: string): Promise<Queue[]> {
    const params = new URLSearchParams({
      limit: limit.toString(),
      offset: offset.toString()
    });
    
    if (namePattern) {
      params.append('namePattern', namePattern);
    }

    const response: AxiosResponse<ApiResponse<Queue[]>> = await axios.get(
      `${API_BASE_URL}/api/v1/queues?${params.toString()}`,
      { headers: this.getHeaders() }
    );
    
    if (!response.data.success) {
      throw new Error(response.data.error?.message || 'Failed to fetch queues');
    }
    
    return response.data.data || [];
  }

  /**
   * 큐 생성
   */
  async createQueue(request: CreateQueueRequest): Promise<Queue> {
    const response: AxiosResponse<ApiResponse<Queue>> = await axios.post(
      `${API_BASE_URL}/api/v1/queues`,
      request,
      { headers: this.getHeaders() }
    );
    
    if (!response.data.success) {
      throw new Error(response.data.error?.message || 'Failed to create queue');
    }
    
    return response.data.data!;
  }

  /**
   * 큐 삭제
   */
  async deleteQueue(queueName: string): Promise<string> {
    const response: AxiosResponse<ApiResponse<string>> = await axios.delete(
      `${API_BASE_URL}/api/v1/queues/${encodeURIComponent(queueName)}`,
      { headers: this.getHeaders() }
    );
    
    if (!response.data.success) {
      throw new Error(response.data.error?.message || 'Failed to delete queue');
    }
    
    return response.data.data!;
  }

  /**
   * 큐 속성 조회
   */
  async getQueueAttributes(queueName: string): Promise<Queue> {
    const response: AxiosResponse<ApiResponse<Queue>> = await axios.get(
      `${API_BASE_URL}/api/v1/queues/${encodeURIComponent(queueName)}/attributes`,
      { headers: this.getHeaders() }
    );
    
    if (!response.data.success) {
      throw new Error(response.data.error?.message || 'Failed to fetch queue attributes');
    }
    
    return response.data.data!;
  }

  /**
   * 큐 통계 조회
   */
  async getQueueStatistics(queueName: string): Promise<QueueStatistics> {
    const response: AxiosResponse<ApiResponse<QueueStatistics>> = await axios.get(
      `${API_BASE_URL}/api/v1/queues/${encodeURIComponent(queueName)}/stats`,
      { headers: this.getHeaders() }
    );
    
    if (!response.data.success) {
      throw new Error(response.data.error?.message || 'Failed to fetch queue statistics');
    }
    
    return response.data.data!;
  }

  /**
   * 메시지 전송
   */
  async sendMessage(queueName: string, request: SendMessageRequest): Promise<SendMessageResponse> {
    const response: AxiosResponse<ApiResponse<SendMessageResponse>> = await axios.post(
      `${API_BASE_URL}/api/v1/queues/${encodeURIComponent(queueName)}/messages`,
      request,
      { headers: this.getHeaders() }
    );
    
    if (!response.data.success) {
      throw new Error(response.data.error?.message || 'Failed to send message');
    }
    
    return response.data.data!;
  }

  /**
   * 메시지 일괄 전송
   */
  async sendMessageBatch(queueName: string, request: SendMessageBatchRequest): Promise<SendMessageBatchResponse> {
    const response: AxiosResponse<ApiResponse<SendMessageBatchResponse>> = await axios.post(
      `${API_BASE_URL}/api/v1/queues/${encodeURIComponent(queueName)}/messages/batch`,
      request,
      { headers: this.getHeaders() }
    );
    
    if (!response.data.success) {
      throw new Error(response.data.error?.message || 'Failed to send batch messages');
    }
    
    return response.data.data!;
  }

  /**
   * 메시지 수신 (처리 중 상태로 이동)
   */
  async receiveMessages(queueName: string, request?: ReceiveMessageRequest): Promise<ReceiveMessageResponse> {
    const params = new URLSearchParams();
    if (request?.maxNumberOfMessages) {
      params.append('maxNumberOfMessages', request.maxNumberOfMessages.toString());
    }
    if (request?.visibilityTimeoutSeconds) {
      params.append('visibilityTimeoutSeconds', request.visibilityTimeoutSeconds.toString());
    }
    if (request?.waitTimeSeconds) {
      params.append('waitTimeSeconds', request.waitTimeSeconds.toString());
    }
    if (request?.messageGroupId) {
      params.append('messageGroupId', request.messageGroupId);
    }

    const response: AxiosResponse<ApiResponse<ReceiveMessageResponse>> = await axios.get(
      `${API_BASE_URL}/api/v1/queues/${encodeURIComponent(queueName)}/messages?${params.toString()}`,
      { headers: this.getHeaders() }
    );
    
    if (!response.data.success) {
      throw new Error(response.data.error?.message || 'Failed to receive messages');
    }
    
    return response.data.data!;
  }

  /**
   * 메시지 미리보기 (큐에서 제거하지 않음)
   */
  async peekMessages(queueName: string, maxMessages: number = 10): Promise<PeekMessageResponse> {
    const response: AxiosResponse<ApiResponse<PeekMessageResponse>> = await axios.get(
      `${API_BASE_URL}/api/v1/queues/${encodeURIComponent(queueName)}/messages/peek?maxMessages=${maxMessages}`,
      { headers: this.getHeaders() }
    );
    
    if (!response.data.success) {
      throw new Error(response.data.error?.message || 'Failed to peek messages');
    }
    
    return response.data.data!;
  }

  /**
   * 메시지 삭제
   */
  async deleteMessage(queueName: string, receiptHandle: string): Promise<string> {
    const response: AxiosResponse<ApiResponse<string>> = await axios.delete(
      `${API_BASE_URL}/api/v1/queues/${encodeURIComponent(queueName)}/messages/${encodeURIComponent(receiptHandle)}`,
      { headers: this.getHeaders() }
    );
    
    if (!response.data.success) {
      throw new Error(response.data.error?.message || 'Failed to delete message');
    }
    
    return response.data.data!;
  }

  /**
   * 큐 비우기 (모든 메시지 삭제)
   */
  async purgeQueue(queueName: string): Promise<string> {
    const response: AxiosResponse<ApiResponse<string>> = await axios.delete(
      `${API_BASE_URL}/api/v1/queues/${encodeURIComponent(queueName)}/messages`,
      { headers: this.getHeaders() }
    );
    
    if (!response.data.success) {
      throw new Error(response.data.error?.message || 'Failed to purge queue');
    }
    
    return response.data.data!;
  }

  /**
   * Enhanced functionality - Basic implementations that work with existing backend
   */

  // Simple search function that uses peek with filtering
  async searchMessages(queueName: string, searchRequest: any): Promise<any> {
    // For now, implement as enhanced peek with basic filtering
    try {
      const messages = await this.peekMessages(queueName, searchRequest.pagination?.limit || 20);
      
      let filteredMessages = messages.messages;
      
      // Basic content filtering
      if (searchRequest.contentFilter) {
        const searchTerm = searchRequest.contentFilter.toLowerCase();
        filteredMessages = filteredMessages.filter(msg => 
          msg.messageBody.toLowerCase().includes(searchTerm)
        );
      }
      
      // Basic attribute filtering  
      if (searchRequest.attributeFilters) {
        Object.entries(searchRequest.attributeFilters).forEach(([key, value]) => {
          filteredMessages = filteredMessages.filter(msg => 
            msg.attributes[key] && msg.attributes[key].includes(String(value).replace(/[!*]/g, ''))
          );
        });
      }
      
      return {
        messages: filteredMessages,
        totalCount: filteredMessages.length,
        offset: 0,
        limit: searchRequest.pagination?.limit || 20,
        hasMore: false,
        searchMetadata: {
          searchTimeMs: 50,
          tiersSearched: ['HOT'],
          filterApplied: ['basic'],
          indexesUsed: ['none']
        }
      };
    } catch (error) {
      throw new Error('Search failed: ' + (error as Error).message);
    }
  }

  // Basic bulk operations using existing delete functionality
  async bulkOperation(queueName: string, bulkRequest: any): Promise<any> {
    const startTime = Date.now();
    let successCount = 0;
    let failureCount = 0;
    const errors: any[] = [];
    
    try {
      if (bulkRequest.operation === 'DELETE_BY_IDS' && bulkRequest.messageIds) {
        // Since we don't have receipt handles, we can't actually delete by ID
        // This is a limitation of the current backend
        throw new Error('Delete by ID requires receipt handles, not available in current implementation');
      }
      
      if (bulkRequest.operation === 'PURGE_EXPIRED') {
        await this.purgeQueue(queueName);
        successCount = 1;
      }
      
      return {
        operation: bulkRequest.operation,
        totalRequested: bulkRequest.messageIds?.length || 1,
        successCount,
        failureCount,
        errors,
        processingTimeMs: Date.now() - startTime
      };
    } catch (error) {
      return {
        operation: bulkRequest.operation,
        totalRequested: bulkRequest.messageIds?.length || 1,
        successCount: 0,
        failureCount: 1,
        errors: [{ errorCode: 'OPERATION_FAILED', errorMessage: (error as Error).message }],
        processingTimeMs: Date.now() - startTime
      };
    }
  }

  // Mock storage tier functionality since backend doesn't have these endpoints yet
  async getTierMetrics(tier: string): Promise<any> {
    // Return mock data for now
    return {
      success: true,
      data: {
        tier,
        messageCount: Math.floor(Math.random() * 1000),
        storageUsageBytes: Math.floor(Math.random() * 1000000),
        avgAccessTimeMs: tier === 'HOT' ? 1 : tier === 'WARM' ? 50 : 200,
        hitRate: Math.random() * 0.3 + 0.7,
        migrationRate: Math.random() * 10,
        errorRate: Math.random() * 2,
        lastUpdated: new Date().toISOString()
      }
    };
  }

  async getStorageUsage(tier: string): Promise<any> {
    const totalCapacity = 1000000000; // 1GB
    const usedBytes = Math.floor(Math.random() * totalCapacity * 0.8);
    
    return {
      success: true,
      data: {
        tier,
        totalCapacityBytes: totalCapacity,
        usedBytes,
        availableBytes: totalCapacity - usedBytes,
        usagePercent: (usedBytes / totalCapacity) * 100,
        messageCount: Math.floor(Math.random() * 500)
      }
    };
  }

  async getAllTierMetrics(): Promise<any> {
    const tiers = ['HOT', 'WARM', 'COLD'];
    const metrics = await Promise.all(
      tiers.map(tier => this.getTierMetrics(tier).then(res => res.data))
    );
    
    return {
      success: true,
      data: metrics
    };
  }

  async getMessagesByTier(queueName: string, tier: string, limit: number = 50): Promise<any> {
    // Use peek messages as a simulation
    try {
      const messages = await this.peekMessages(queueName, Math.min(limit, 10));
      
      // Add tier information to messages
      const enhancedMessages = messages.messages.map(msg => ({
        ...msg,
        currentTier: tier,
        migrationHistory: [],
        accessCount: Math.floor(Math.random() * 5),
        lastAccessTime: new Date().toISOString()
      }));
      
      return {
        success: true,
        data: enhancedMessages
      };
    } catch (error) {
      return {
        success: false,
        error: { message: (error as Error).message }
      };
    }
  }

  async migrateMessages(fromTier: string, toTier: string, messageIds?: string[]): Promise<any> {
    // Mock migration - in real implementation this would move messages between storage tiers
    const startTime = Date.now();
    const successCount = messageIds?.length || Math.floor(Math.random() * 10) + 1;
    
    return {
      success: true,
      data: {
        operation: 'MIGRATE',
        totalRequested: successCount,
        successCount,
        failureCount: 0,
        errors: [],
        processingTimeMs: Date.now() - startTime
      }
    };
  }

  // Enhanced message sending with better batch processing
  async sendMessageBatchEnhanced(queueName: string, request: any): Promise<any> {
    try {
      // Use existing batch functionality
      const result = await this.sendMessageBatch(queueName, request);
      
      return {
        success: true,
        data: result
      };
    } catch (error) {
      return {
        success: false,
        error: { message: (error as Error).message }
      };
    }
  }
}

const queueService = new QueueService();
export default queueService;
