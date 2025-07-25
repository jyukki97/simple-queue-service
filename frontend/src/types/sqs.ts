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

export interface QueueStatistics {
  queueName: string;
  approximateNumberOfMessages: number;
  approximateNumberOfMessagesNotVisible: number;
  approximateNumberOfMessagesInDLQ: number;
  approximateAgeOfOldestMessage: number | null;
  lastUpdated: number;
}

export interface MessagePreview {
  messageId: string;
  messageBody: string;
  attributes: { [key: string]: string };
  sentTimestamp: number;
  firstReceiveTimestamp?: number;
  receiveCount: number;
  approximateFirstReceiveTimestamp?: number;
}

// Enhanced types for new features
export interface MessageSearchRequest {
  attributeFilters?: { [key: string]: string };
  contentFilter?: string;
  timeRange?: TimeRange;
  tier?: StorageTier;
  messageGroupId?: string;
  priority?: PriorityRange;
  receiveCountRange?: CountRange;
  sorting?: MessageSorting;
  responseFormat?: ResponseFormat;
  pagination?: Pagination;
}

export interface TimeRange {
  startTime: string; // ISO string
  endTime: string; // ISO string
}

export interface PriorityRange {
  minPriority?: number;
  maxPriority?: number;
}

export interface CountRange {
  minCount?: number;
  maxCount?: number;
}

export interface MessageSorting {
  sortBy?: SortField;
  sortOrder?: SortOrder;
}

export enum SortField {
  SENT_TIME = 'SENT_TIME',
  FIRST_RECEIVE_TIME = 'FIRST_RECEIVE_TIME',
  RECEIVE_COUNT = 'RECEIVE_COUNT',
  PRIORITY = 'PRIORITY',
  MESSAGE_SIZE = 'MESSAGE_SIZE',
  MESSAGE_GROUP_ID = 'MESSAGE_GROUP_ID'
}

export enum SortOrder {
  ASC = 'ASC',
  DESC = 'DESC'
}

export enum ResponseFormat {
  STANDARD = 'STANDARD',
  DETAILED = 'DETAILED',
  MINIMAL = 'MINIMAL',
  CUSTOM = 'CUSTOM'
}

export interface Pagination {
  limit?: number;
  offset?: number;
}

export enum StorageTier {
  HOT = 'HOT',
  WARM = 'WARM',
  COLD = 'COLD'
}

export interface MessageSearchResponse {
  messages: MessageResponse[];
  totalCount: number;
  offset: number;
  limit: number;
  hasMore: boolean;
  searchMetadata?: SearchMetadata;
}

export interface SearchMetadata {
  searchTimeMs: number;
  tiersSearched: string[];
  filterApplied: string[];
  indexesUsed: string[];
}

// Bulk operations
export interface BulkOperationRequest {
  operation: BulkOperationType;
  messageIds?: string[];
  filters?: MessageFilter;
  parameters?: { [key: string]: any };
}

export enum BulkOperationType {
  DELETE_BY_IDS = 'DELETE_BY_IDS',
  DELETE_BY_FILTER = 'DELETE_BY_FILTER',
  CHANGE_VISIBILITY_BY_IDS = 'CHANGE_VISIBILITY_BY_IDS',
  MOVE_BY_FILTER = 'MOVE_BY_FILTER',
  PURGE_EXPIRED = 'PURGE_EXPIRED'
}

export interface BulkOperationResponse {
  operation: BulkOperationType;
  totalRequested: number;
  successCount: number;
  failureCount: number;
  errors: BulkOperationError[];
  processingTimeMs: number;
}

export interface BulkOperationError {
  messageId?: string;
  errorCode: string;
  errorMessage: string;
}

export interface MessageFilter {
  attributeFilters?: { [key: string]: string };
  contentFilter?: string;
  timeRange?: TimeRange;
  messageGroupId?: string;
  minReceiveCount?: number;
  maxReceiveCount?: number;
  currentTier?: StorageTier;
}

// Storage tier information
export interface TierMetrics {
  tier: StorageTier;
  messageCount: number;
  storageUsageBytes: number;
  avgAccessTimeMs: number;
  hitRate: number;
  migrationRate: number;
  errorRate: number;
  lastUpdated: string; // ISO string
}

export interface StorageUsageInfo {
  tier: StorageTier;
  totalCapacityBytes: number;
  usedBytes: number;
  availableBytes: number;
  usagePercent: number;
  messageCount: number;
}

// Enhanced message response with tier information
export interface EnhancedMessageResponse extends MessageResponse {
  currentTier?: StorageTier;
  migrationHistory?: TierMigration[];
  accessCount?: number;
  lastAccessTime?: string;
}

export interface TierMigration {
  fromTier: StorageTier;
  toTier: StorageTier;
  migrationTime: string;
  reason: string;
}

// Filter builder types for UI
export interface FilterCriteria {
  id: string;
  type: FilterType;
  field: string;
  operator: FilterOperator;
  value: string | number | boolean;
  enabled: boolean;
}

export enum FilterType {
  ATTRIBUTE = 'ATTRIBUTE',
  CONTENT = 'CONTENT',
  TIME_RANGE = 'TIME_RANGE',
  PRIORITY = 'PRIORITY',
  RECEIVE_COUNT = 'RECEIVE_COUNT',
  TIER = 'TIER',
  MESSAGE_GROUP = 'MESSAGE_GROUP'
}

export enum FilterOperator {
  EQUALS = 'EQUALS',
  NOT_EQUALS = 'NOT_EQUALS',
  CONTAINS = 'CONTAINS',
  NOT_CONTAINS = 'NOT_CONTAINS',
  STARTS_WITH = 'STARTS_WITH',
  ENDS_WITH = 'ENDS_WITH',
  REGEX = 'REGEX',
  GREATER_THAN = 'GREATER_THAN',
  LESS_THAN = 'LESS_THAN',
  BETWEEN = 'BETWEEN',
  IN = 'IN',
  NOT_IN = 'NOT_IN'
}
