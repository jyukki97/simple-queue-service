// SQS API 타입 정의
export interface ApiResponse<T> {
  success: boolean;
  data?: T;
  error?: {
    code: string;
    message: string;
  };
  timestamp: number;
}

export interface Queue {
  tenantId: string;
  queueName: string;
  attributes: QueueAttributes;
  createdAt: number;
  updatedAt: number;
}

export interface QueueAttributes {
  visibilityTimeoutSeconds: number;
  messageRetentionPeriod: number;
  delaySeconds: number;
  maxReceiveCount: number;
  deadLetterTargetArn?: string;
  redrivePolicy?: any;
}

export interface CreateQueueRequest {
  queueName: string;
  attributes: QueueAttributes;
}

export interface SendMessageRequest {
  messageBody: string;
  delaySeconds?: number;
  attributes?: { [key: string]: string };
}

export interface SendMessageResponse {
  messageId: string;
  messageBodyMD5?: string;
}

export interface BatchMessageRequest {
  id: string;
  messageBody: string;
  delaySeconds?: number;
  attributes?: { [key: string]: string };
}

export interface SendMessageBatchRequest {
  messages: BatchMessageRequest[];
}

export interface BatchMessageResponse {
  id: string;
  messageId: string;
  messageBodyMD5?: string;
}

export interface BatchMessageFailure {
  id: string;
  code: string;
  message: string;
  senderFault?: boolean;
}

export interface SendMessageBatchResponse {
  successful: BatchMessageResponse[];
  failed: BatchMessageFailure[];
}

export interface MessageResponse {
  messageId: string;
  messageBody: string;
  attributes: { [key: string]: string };
  receiptHandle: string;
  sentTimestamp: number;
}

export interface DeleteMessageBatchRequest {
  receiptHandles: string[];
}

export interface DeleteMessageBatchResponse {
  successful: string[];
  failed: BatchMessageFailure[];
}
