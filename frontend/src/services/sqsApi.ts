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
} from '../types/sqs';

const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:8080';
const API_KEY = 'test-api-key';

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
    'X-API-Key': API_KEY,
  },
});

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
};
