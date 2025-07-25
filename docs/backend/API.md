# API Reference - Simple Queue Service

## Authentication

All API requests require an API key to be passed in the `x-api-key` header.

```
x-api-key: test-api-key
```

### Available API Keys
- **Admin**: `admin-api-key` (full system access)
- **Test Tenant**: `test-api-key` (tenant-scoped access)

## Queue Management API

### Create Queue
```http
POST /api/queues
Content-Type: application/json
x-api-key: test-api-key

{
  "queueName": "my-queue",
  "queueType": "STANDARD",  // or "FIFO"
  "attributes": {
    "visibilityTimeoutSeconds": 30,
    "messageRetentionPeriod": 1209600,
    "delaySeconds": 0,
    "maxReceiveCount": 3,
    "deadLetterTargetArn": "my-dlq"
  }
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "queueName": "my-queue",
    "queueUrl": "http://localhost:8080/api/queues/my-queue",
    "attributes": {
      "visibilityTimeoutSeconds": 30,
      "messageRetentionPeriod": 1209600,
      "createdTimestamp": "2024-01-01T00:00:00Z"
    }
  }
}
```

### List Queues
```http
GET /api/queues?prefix=my&limit=10
x-api-key: test-api-key
```

**Response:**
```json
{
  "success": true,
  "data": {
    "queueUrls": [
      "http://localhost:8080/api/queues/my-queue-1",
      "http://localhost:8080/api/queues/my-queue-2"
    ],
    "nextToken": "eyJ0b2tlbiI6Im5leHQtcGFnZSJ9"
  }
}
```

### Get Queue Attributes
```http
GET /api/queues/my-queue
x-api-key: test-api-key
```

**Response:**
```json
{
  "success": true,
  "data": {
    "queueName": "my-queue",
    "queueType": "STANDARD",
    "attributes": {
      "visibilityTimeoutSeconds": 30,
      "messageRetentionPeriod": 1209600,
      "approximateNumberOfMessages": 42,
      "approximateNumberOfMessagesNotVisible": 5,
      "createdTimestamp": "2024-01-01T00:00:00Z",
      "lastModifiedTimestamp": "2024-01-01T12:00:00Z"
    }
  }
}
```

### Delete Queue
```http
DELETE /api/queues/my-queue
x-api-key: test-api-key
```

### Purge Queue
```http
POST /api/queues/my-queue/purge
x-api-key: test-api-key
```

## Message Operations API

### Send Message
```http
POST /api/queues/my-queue/messages
Content-Type: application/json
x-api-key: test-api-key

{
  "messageBody": "Hello, World!",
  "delaySeconds": 0,
  "attributes": {
    "priority": "high",
    "source": "web-app"
  },
  "messageGroupId": "group-1",  // FIFO only
  "messageDeduplicationId": "unique-123"  // FIFO only
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "messageId": "12345678-1234-1234-1234-123456789012",
    "md5OfBody": "ed076287532e86365e841e92bfc50d8c",
    "md5OfAttributes": "d41d8cd98f00b204e9800998ecf8427e",
    "sequenceNumber": "18849496460467696128"  // FIFO only
  }
}
```

### Send Message Batch
```http
POST /api/queues/my-queue/messages/batch
Content-Type: application/json
x-api-key: test-api-key

{
  "messages": [
    {
      "id": "msg1",
      "messageBody": "First message",
      "delaySeconds": 0,
      "attributes": {"type": "order"}
    },
    {
      "id": "msg2",
      "messageBody": "Second message",
      "delaySeconds": 5,
      "attributes": {"type": "notification"}
    }
  ]
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "successful": [
      {
        "id": "msg1",
        "messageId": "12345678-1234-1234-1234-123456789012",
        "md5OfBody": "ed076287532e86365e841e92bfc50d8c"
      }
    ],
    "failed": [
      {
        "id": "msg2",
        "senderFault": true,
        "code": "InvalidParameterValue",
        "message": "Message body too large"
      }
    ]
  }
}
```

### Receive Messages
```http
GET /api/queues/my-queue/messages?maxNumberOfMessages=10&visibilityTimeoutSeconds=30&waitTimeSeconds=20
x-api-key: test-api-key
```

**Response:**
```json
{
  "success": true,
  "data": [
    {
      "messageId": "12345678-1234-1234-1234-123456789012",
      "receiptHandle": "receipt-handle-string",
      "md5OfBody": "ed076287532e86365e841e92bfc50d8c",
      "messageBody": "Hello, World!",
      "attributes": {
        "priority": "high",
        "source": "web-app"
      },
      "sentTimestamp": "2024-01-01T12:00:00Z",
      "firstReceiveTimestamp": "2024-01-01T12:01:00Z",
      "receiveCount": 1
    }
  ]
}
```

### Delete Message
```http
DELETE /api/queues/my-queue/messages
Content-Type: application/json
x-api-key: test-api-key

{
  "receiptHandle": "receipt-handle-string"
}
```

### Delete Message Batch
```http
POST /api/queues/my-queue/messages/batch-delete
Content-Type: application/json
x-api-key: test-api-key

{
  "receiptHandles": [
    "receipt-handle-1",
    "receipt-handle-2",
    "receipt-handle-3"
  ]
}
```

**Response:**
```json
{
  "success": true,
  "data": {
    "successful": [
      {"receiptHandle": "receipt-handle-1"},
      {"receiptHandle": "receipt-handle-2"}
    ],
    "failed": [
      {
        "receiptHandle": "receipt-handle-3",
        "senderFault": false,
        "code": "ReceiptHandleIsInvalid",
        "message": "The receipt handle is invalid"
      }
    ]
  }
}
```

### Peek Messages (Preview without receiving)
```http
GET /api/queues/my-queue/messages/peek?limit=5
x-api-key: test-api-key
```

## Storage Management API

### Get Storage Statistics
```http
GET /api/storage/stats
x-api-key: admin-api-key
```

**Response:**
```json
{
  "success": true,
  "data": {
    "hot": {
      "messageCount": 1500,
      "totalSize": 1048576,
      "averageSize": 699,
      "oldestMessage": "2024-01-01T11:30:00Z",
      "newestMessage": "2024-01-01T12:00:00Z"
    },
    "warm": {
      "messageCount": 5000,
      "totalSize": 10485760,
      "averageSize": 2097,
      "oldestMessage": "2024-01-01T08:00:00Z",
      "newestMessage": "2024-01-01T11:30:00Z"
    },
    "cold": {
      "messageCount": 50000,
      "totalSize": 104857600,
      "averageSize": 2097,
      "oldestMessage": "2023-12-15T00:00:00Z",
      "newestMessage": "2024-01-01T08:00:00Z"
    }
  }
}
```

### Manual Tier Promotion
```http
POST /api/storage/tier/promote
Content-Type: application/json
x-api-key: admin-api-key

{
  "messageId": "12345678-1234-1234-1234-123456789012",
  "targetTier": "HOT"  // HOT, WARM, or COLD
}
```

### List Cold Storage Messages
```http
GET /api/storage/cold/messages?queueName=my-queue&limit=100&startAfter=2024-01-01T00:00:00Z
x-api-key: admin-api-key
```

## Admin & Monitoring API

### System Dashboard
```http
GET /api/admin/dashboard
x-api-key: admin-api-key
```

**Response:**
```json
{
  "success": true,
  "data": {
    "systemStats": {
      "totalQueues": 25,
      "totalMessages": 56500,
      "activeConnections": 42,
      "systemUptime": 86400,
      "memoryUsage": {
        "used": 512000000,
        "max": 1073741824,
        "percentage": 47.7
      }
    },
    "storageStats": {
      "hot": {"messageCount": 1500, "totalSize": 1048576},
      "warm": {"messageCount": 5000, "totalSize": 10485760},
      "cold": {"messageCount": 50000, "totalSize": 104857600}
    },
    "recentActivity": [
      {
        "timestamp": "2024-01-01T12:00:00Z",
        "type": "QUEUE_CREATED",
        "details": "Queue 'orders' created by tenant 'test'"
      }
    ]
  }
}
```

### Tenant Management
```http
GET /api/admin/tenants
x-api-key: admin-api-key
```

**Response:**
```json
{
  "success": true,
  "data": [
    {
      "tenantId": "test-tenant",
      "apiKey": "test-api-key",
      "status": "ACTIVE",
      "createdAt": "2024-01-01T00:00:00Z",
      "lastActivity": "2024-01-01T12:00:00Z",
      "usage": {
        "queues": 5,
        "messages": 1500,
        "storage": 1048576
      }
    }
  ]
}
```

### Create Tenant
```http
POST /api/admin/tenants
Content-Type: application/json
x-api-key: admin-api-key

{
  "tenantId": "new-tenant",
  "name": "New Tenant Company"
}
```

## Health & Metrics API

### Health Check
```http
GET /actuator/health
```

**Response:**
```json
{
  "status": "UP",
  "components": {
    "redis": {
      "status": "UP",
      "details": {"version": "7.0.0"}
    },
    "storage": {
      "status": "UP",
      "details": {
        "hot": "UP",
        "warm": "UP", 
        "cold": "UP"
      }
    }
  }
}
```

### Metrics
```http
GET /actuator/metrics
```

### Prometheus Metrics
```http
GET /actuator/prometheus
```

## Error Responses

All error responses follow this format:

```json
{
  "success": false,
  "error": {
    "code": "QUEUE_NOT_FOUND",
    "message": "The specified queue does not exist",
    "details": {
      "queueName": "non-existent-queue",
      "timestamp": "2024-01-01T12:00:00Z"
    }
  }
}
```

### Common Error Codes

| Code | HTTP Status | Description |
|------|-------------|-------------|
| `UNAUTHORIZED` | 401 | Invalid or missing API key |
| `FORBIDDEN` | 403 | Insufficient permissions |
| `QUEUE_NOT_FOUND` | 404 | Queue does not exist |
| `MESSAGE_NOT_FOUND` | 404 | Message not found or expired |
| `INVALID_PARAMETER` | 400 | Invalid request parameter |
| `MESSAGE_TOO_LARGE` | 400 | Message exceeds size limit |
| `BATCH_TOO_LARGE` | 400 | Batch contains too many messages |
| `QUEUE_ALREADY_EXISTS` | 409 | Queue with name already exists |
| `INTERNAL_SERVER_ERROR` | 500 | Unexpected server error |

## Rate Limits

| Operation | Limit | Window |
|-----------|-------|--------|
| Send Message | 300/min | Per queue |
| Receive Message | 1000/min | Per queue |
| Create Queue | 10/min | Per tenant |
| Delete Queue | 5/min | Per tenant |
| Admin Operations | 100/min | Per admin key |

## Request/Response Examples

### FIFO Queue Example

```bash
# Create FIFO queue
curl -X POST http://localhost:8080/api/queues \
  -H "Content-Type: application/json" \
  -H "x-api-key: test-api-key" \
  -d '{
    "queueName": "orders.fifo",
    "queueType": "FIFO",
    "attributes": {
      "visibilityTimeoutSeconds": 60,
      "contentBasedDeduplication": true
    }
  }'

# Send FIFO message
curl -X POST http://localhost:8080/api/queues/orders.fifo/messages \
  -H "Content-Type: application/json" \
  -H "x-api-key: test-api-key" \
  -d '{
    "messageBody": "Order #12345",
    "messageGroupId": "customer-123",
    "messageDeduplicationId": "order-12345-v1",
    "attributes": {
      "customerId": "123",
      "orderValue": "99.99"
    }
  }'
```

### Batch Operations Example

```bash
# Send multiple messages
curl -X POST http://localhost:8080/api/queues/notifications/messages/batch \
  -H "Content-Type: application/json" \
  -H "x-api-key: test-api-key" \
  -d '{
    "messages": [
      {
        "id": "notif1",
        "messageBody": "Welcome email",
        "attributes": {"type": "email", "priority": "low"}
      },
      {
        "id": "notif2", 
        "messageBody": "Payment confirmation",
        "attributes": {"type": "sms", "priority": "high"}
      }
    ]
  }'
```