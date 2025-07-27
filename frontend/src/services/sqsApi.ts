import axios from 'axios';
import {
  ApiResponse,
  Queue,
  CreateQueueRequest,
  SendMessageRequest,
  SendMessageResponse,
  SendMessageBatchRequest,
  SendMessageBatchResponse,
  MessageResponse,
  DeleteMessageBatchRequest,
  DeleteMessageBatchResponse,
  QueueStatistics,
  MessagePreview,
  MessageSearchRequest,
  MessageSearchResponse,
  BulkOperationRequest,
  BulkOperationResponse,
  TierMetrics,
  StorageUsageInfo,
  EnhancedMessageResponse,
  StorageTier,
} from '../types/sqs';
import configService from '../utils/config';

const apiConfig = configService.getApiConfig();

const api = axios.create({
  baseURL: apiConfig.baseUrl,
  timeout: apiConfig.timeout,
  headers: {
    'Content-Type': 'application/json',
    'X-API-Key': apiConfig.key,
  },
});

// Export the configured api instance for use in other services
export { api as apiClient };

// Add request/response interceptors for centralized error handling
api.interceptors.request.use(
  (config) => {
    if (configService.getUIConfig().debugMode) {
      console.debug('API Request:', config.method?.toUpperCase(), config.url);
    }
    return config;
  },
  (error) => {
    console.error('API Request Error:', error);
    return Promise.reject(error);
  }
);

api.interceptors.response.use(
  (response) => {
    if (configService.getUIConfig().debugMode) {
      console.debug('API Response:', response.status, response.config.url);
    }
    return response;
  },
  (error) => {
    console.error('API Response Error:', error.response?.status, error.response?.data);
    return Promise.reject(error);
  }
);

export const sqsApi = {
  // 큐 관리
  createQueue: async (request: CreateQueueRequest): Promise<ApiResponse<Queue>> => {
    const response = await api.post('/api/v1/queues', request);
    return response.data;
  },

  listQueues: async (): Promise<ApiResponse<Queue[]>> => {
    const response = await api.get('/api/v1/queues');
    return response.data;
  },

  deleteQueue: async (queueName: string): Promise<ApiResponse<string>> => {
    const response = await api.delete(`/api/v1/queues/${queueName}`);
    return response.data;
  },

  // 메시지 관리
  sendMessage: async (
    queueName: string,
    request: SendMessageRequest
  ): Promise<ApiResponse<SendMessageResponse>> => {
    const response = await api.post(`/api/v1/queues/${queueName}/messages`, request);
    return response.data;
  },

  sendMessageBatch: async (
    queueName: string,
    request: SendMessageBatchRequest
  ): Promise<ApiResponse<SendMessageBatchResponse>> => {
    const response = await api.post(`/api/v1/queues/${queueName}/messages/batch`, request);
    return response.data;
  },

  receiveMessages: async (
    queueName: string,
    maxMessages: number = 1
  ): Promise<ApiResponse<MessageResponse[]>> => {
    const response = await api.get(
      `/api/v1/queues/${queueName}/messages?maxNumberOfMessages=${maxMessages}`
    );
    return response.data;
  },

  deleteMessage: async (queueName: string, receiptHandle: string): Promise<ApiResponse<string>> => {
    const response = await api.delete(`/api/v1/queues/${queueName}/messages/${receiptHandle}`);
    return response.data;
  },

  deleteMessageBatch: async (
    queueName: string,
    request: DeleteMessageBatchRequest
  ): Promise<ApiResponse<DeleteMessageBatchResponse>> => {
    const response = await api.post(`/api/v1/queues/${queueName}/messages/delete-batch`, request);
    return response.data;
  },

  purgeQueue: async (queueName: string): Promise<ApiResponse<string>> => {
    const response = await api.delete(`/api/v1/queues/${queueName}/messages`);
    return response.data;
  },

  getQueueStatistics: async (queueName: string): Promise<ApiResponse<QueueStatistics>> => {
    const response = await api.get(`/api/v1/queues/${queueName}/statistics`);
    return response.data;
  },

  peekMessages: async (
    queueName: string,
    maxMessages: number = 10
  ): Promise<ApiResponse<MessagePreview[]>> => {
    const response = await api.get(`/api/v1/queues/${queueName}/messages/peek`, {
      params: { maxMessages },
    });
    return response.data;
  },

  // Enhanced search and filtering endpoints
  searchMessages: async (
    queueName: string,
    searchRequest: MessageSearchRequest
  ): Promise<ApiResponse<MessageSearchResponse>> => {
    const response = await api.post(`/api/v1/queues/${queueName}/messages/search`, searchRequest);
    return response.data;
  },

  // Bulk operations
  bulkOperation: async (
    queueName: string,
    bulkRequest: BulkOperationRequest
  ): Promise<ApiResponse<BulkOperationResponse>> => {
    const response = await api.post(`/api/v1/queues/${queueName}/messages/bulk`, bulkRequest);
    return response.data;
  },

  // Storage tier endpoints
  getTierMetrics: async (tier: StorageTier): Promise<ApiResponse<TierMetrics>> => {
    const response = await api.get(`/api/v1/storage/tiers/${tier}/metrics`);
    return response.data;
  },

  getStorageUsage: async (tier: StorageTier): Promise<ApiResponse<StorageUsageInfo>> => {
    const response = await api.get(`/api/v1/storage/tiers/${tier}/usage`);
    return response.data;
  },

  getAllTierMetrics: async (): Promise<ApiResponse<TierMetrics[]>> => {
    const response = await api.get('/api/v1/storage/tiers/metrics');
    return response.data;
  },

  // Cross-tier message lookup
  findMessageAcrossTiers: async (
    messageId: string
  ): Promise<ApiResponse<EnhancedMessageResponse>> => {
    const response = await api.get(`/api/v1/storage/messages/${messageId}`);
    return response.data;
  },

  // Get messages by tier
  getMessagesByTier: async (
    queueName: string,
    tier: StorageTier,
    limit: number = 50
  ): Promise<ApiResponse<EnhancedMessageResponse[]>> => {
    const response = await api.get(`/api/v1/storage/queues/${queueName}/messages/by-tier`, {
      params: { tier, limit },
    });
    return response.data;
  },

  // Migration operations
  migrateMessages: async (
    fromTier: StorageTier,
    toTier: StorageTier,
    messageIds?: string[]
  ): Promise<ApiResponse<BulkOperationResponse>> => {
    const response = await api.post('/api/v1/storage/migrate', {
      fromTier,
      toTier,
      messageIds,
    });
    return response.data;
  },

  // Performance monitoring
  getPerformanceMetrics: async (
    timeRange?: { start: string; end: string }
  ): Promise<ApiResponse<any>> => {
    const response = await api.get('/api/v1/monitoring/performance', {
      params: timeRange,
    });
    return response.data;
  },
};
