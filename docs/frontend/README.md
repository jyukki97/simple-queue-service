# Frontend Documentation - Simple Queue Service Admin Dashboard

## Overview

The Simple Queue Service frontend is a **React + TypeScript** admin dashboard that provides a comprehensive interface for managing queues, messages, tenants, and monitoring system performance. It's designed as a single-page application (SPA) with multiple views for different administrative functions.

## Technology Stack

- **Framework**: React 19.1.0
- **Language**: TypeScript 4.9.5
- **Routing**: React Router DOM 6.29.0
- **HTTP Client**: Axios 1.10.0
- **Charts**: Recharts 2.14.0
- **Build Tool**: React Scripts 5.0.1
- **Testing**: Jest + React Testing Library

## Architecture

### Application Structure

```
src/
├── components/          # React components
├── services/           # API service layers
├── types/             # TypeScript type definitions
├── App.tsx            # Main application component
├── App.css            # Global styles
└── index.tsx          # Application entry point
```

### Component Architecture

The application follows a **component-based architecture** with clear separation of concerns:

- **Page Components**: Main views (Dashboard, QueueManagement, etc.)
- **Feature Components**: Specialized functionality (MessageManager, StorageTierManager)
- **Service Layer**: API communication abstraction
- **Type System**: Comprehensive TypeScript interfaces

## Core Components

### 1. Navigation (`Navigation.tsx`)
- **Purpose**: Main navigation menu
- **Features**: Route-based active state, responsive design
- **Routes**: Dashboard, Queues, Tenants, Monitoring

### 2. Dashboard (`Dashboard.tsx`)
- **Purpose**: System overview and quick stats
- **Features**: 
  - Real-time system statistics
  - Recent activity feed
  - Quick access to common operations
  - Visual charts and metrics

### 3. Queue Management (`QueueManagement.tsx`)
- **Purpose**: Complete queue lifecycle management
- **Features**:
  - Create/delete queues
  - Queue configuration (FIFO/Standard)
  - Queue statistics and monitoring
  - Integration with MessageManager

### 4. Message Manager (`MessageManager.tsx`)
- **Purpose**: Message operations within queues
- **Features**:
  - Send single messages
  - Send batch messages (up to 10)
  - Receive and display messages
  - Delete messages (single/batch)
  - Queue purge functionality
  - Message attribute management

### 5. Enhanced Message Manager (`EnhancedMessageManager.tsx`)
- **Purpose**: Advanced message management
- **Features**:
  - Message filtering and search
  - Advanced message preview
  - Bulk operations
  - Message metadata display

### 6. Advanced Message Search (`AdvancedMessageSearch.tsx`)
- **Purpose**: Complex message querying
- **Features**:
  - Multi-criteria search
  - Date range filtering
  - Attribute-based filtering
  - Export search results

### 7. Intelligent Batch Processor (`IntelligentBatchProcessor.tsx`)
- **Purpose**: Smart batch message processing
- **Features**:
  - Automated batch optimization
  - Progress tracking
  - Error handling and retry logic
  - Performance metrics

### 8. Storage Tier Manager (`StorageTierManager.tsx`)
- **Purpose**: Storage tier monitoring and management
- **Features**:
  - Tier statistics visualization
  - Manual message promotion/demotion
  - Storage usage monitoring
  - Tier performance metrics

### 9. Tenant Management (`TenantManagement.tsx`)
- **Purpose**: Multi-tenant administration
- **Features**:
  - Create/manage tenants
  - API key management
  - Tenant usage statistics
  - Access control

### 10. System Monitoring (`SystemMonitoring.tsx`)
- **Purpose**: System health and performance monitoring
- **Features**:
  - Real-time metrics
  - Performance graphs
  - Alert management
  - System health indicators

## Service Layer

### API Service (`sqsApi.ts`)
- **Purpose**: Core SQS API communication
- **Features**:
  - Queue operations (CRUD)
  - Message operations (send/receive/delete)
  - Batch operations
  - Error handling and response parsing

### Queue Service (`queueService.ts`)
- **Purpose**: Queue-specific business logic
- **Features**:
  - Queue validation
  - Queue statistics aggregation
  - Queue configuration management

### Admin Service (`adminService.ts`)
- **Purpose**: Administrative operations
- **Features**:
  - System statistics
  - Dashboard data aggregation
  - Activity monitoring

### Storage Service (`storageService.ts`)
- **Purpose**: Storage tier management
- **Features**:
  - Tier statistics
  - Storage operations
  - Performance monitoring

### Tenant Service (`tenantService.ts`)
- **Purpose**: Tenant management operations
- **Features**:
  - Tenant CRUD operations
  - Usage tracking
  - API key management

## Type System

### Core Types (`types/sqs.ts`)

```typescript
interface Queue {
  queueName: string;
  queueType: 'STANDARD' | 'FIFO';
  attributes: QueueAttributes;
  createdTimestamp: string;
  lastModifiedTimestamp: string;
}

interface MessageResponse {
  messageId: string;
  receiptHandle: string;
  messageBody: string;
  attributes: Record<string, any>;
  sentTimestamp: string;
  receiveCount: number;
}

interface BatchMessageRequest {
  id: string;
  messageBody: string;
  delaySeconds?: number;
  attributes?: Record<string, any>;
}
```

### Admin Types (`types/admin.ts`)

```typescript
interface SystemStats {
  totalQueues: number;
  totalMessages: number;
  activeConnections: number;
  systemUptime: number;
  memoryUsage: MemoryUsage;
}

interface TenantInfo {
  tenantId: string;
  apiKey: string;
  status: 'ACTIVE' | 'SUSPENDED' | 'DELETED';
  createdAt: string;
  lastActivity: string;
  usage: TenantUsage;
}
```

## Features & Functionality

### Queue Management
- **Create Queues**: Support for both STANDARD and FIFO queues
- **Queue Configuration**: Visibility timeout, message retention, DLQ setup
- **Queue Statistics**: Message counts, size metrics, age distribution
- **Queue Operations**: Purge, delete, attribute modification

### Message Operations
- **Single Message Send**: With attributes, delay, and custom metadata
- **Batch Message Send**: Up to 10 messages per batch with error handling
- **Message Receive**: Configurable visibility timeout and max messages
- **Message Delete**: Single and batch deletion with receipt handle validation
- **Message Preview**: Non-destructive message viewing

### Advanced Features
- **Intelligent Search**: Multi-criteria message search across storage tiers
- **Batch Processing**: Optimized batch operations with progress tracking
- **Storage Management**: Cross-tier message management and promotion
- **Real-time Monitoring**: Live system metrics and performance indicators

### Multi-Tenant Support
- **Tenant Isolation**: Complete separation of tenant resources
- **API Key Management**: Secure key generation and rotation
- **Usage Tracking**: Per-tenant resource utilization monitoring
- **Access Control**: Role-based feature access

## Configuration

### Development Setup

```bash
# Install dependencies
cd frontend && npm install

# Start development server
npm start

# Build for production
npm run build

# Run tests
npm test
```

### Environment Configuration

The frontend is configured to proxy API requests to the backend:

```json
// package.json
{
  "proxy": "http://localhost:8080"
}
```

### API Configuration

API communication uses hardcoded API keys for development:

```typescript
// Default API key for development
const API_KEY = 'test-api-key';
```

**Production Note**: API keys should be configured via environment variables.

## Styling

### CSS Architecture
- **Component-scoped CSS**: Each component has its own CSS file
- **Global Styles**: App.css for application-wide styles
- **Responsive Design**: Mobile-friendly layouts
- **Theme Consistency**: Unified color scheme and typography

### Key Style Features
- **Color Scheme**: Professional blue/gray palette
- **Typography**: Clean, readable fonts
- **Components**: Consistent button, form, and card styles
- **Charts**: Integrated Recharts styling
- **Responsive**: Mobile and tablet friendly

## API Integration

### Request/Response Handling

```typescript
// Example API service method
export const receiveMessages = async (
  queueName: string, 
  maxMessages: number = 10
): Promise<ApiResponse<MessageResponse[]>> => {
  try {
    const response = await axios.get(
      `/api/queues/${queueName}/messages`,
      {
        params: { maxNumberOfMessages: maxMessages },
        headers: { 'x-api-key': API_KEY }
      }
    );
    return { success: true, data: response.data.data };
  } catch (error) {
    return handleApiError(error);
  }
};
```

### Error Handling

```typescript
interface ApiResponse<T> {
  success: boolean;
  data?: T;
  error?: {
    code: string;
    message: string;
    details?: any;
  };
}
```

## State Management

### Component State
- **Local State**: React useState for component-specific data
- **Effect Hooks**: useEffect for API calls and cleanup
- **Custom Hooks**: Reusable stateful logic

### Data Flow
1. **User Action** → Component Event Handler
2. **API Call** → Service Layer
3. **Response Processing** → State Update
4. **UI Update** → Component Re-render

## Testing

### Test Structure
- **Unit Tests**: Component behavior testing
- **Integration Tests**: Service layer testing
- **User Event Tests**: User interaction simulation

### Testing Libraries
- **Jest**: Test runner and assertions
- **React Testing Library**: Component testing utilities
- **User Event**: User interaction simulation

```bash
# Run all tests
npm test

# Run tests in watch mode
npm test -- --watch

# Generate coverage report
npm test -- --coverage
```

## Performance Optimization

### Bundle Optimization
- **Code Splitting**: Route-based lazy loading potential
- **Tree Shaking**: Unused code elimination
- **Minification**: Production build optimization

### Runtime Performance
- **Memoization**: Preventing unnecessary re-renders
- **Efficient Updates**: Minimal DOM manipulation
- **API Caching**: Request deduplication

### Monitoring Integration
- **Web Vitals**: Core performance metrics
- **Error Tracking**: Runtime error monitoring
- **User Analytics**: Usage pattern analysis

## Security Considerations

### API Security
- **API Key Authentication**: All requests authenticated
- **HTTPS**: Secure communication (production)
- **Input Validation**: Client-side validation

### Data Handling
- **Sensitive Data**: No storage of API keys in localStorage
- **XSS Prevention**: React's built-in XSS protection
- **CSRF Protection**: Backend CSRF token support

## Deployment

### Build Process
```bash
# Production build
npm run build

# Build artifacts in build/ directory
# Static files ready for web server deployment
```

### Deployment Options
- **Static Hosting**: Netlify, Vercel, S3 + CloudFront
- **Docker**: Container-based deployment
- **Traditional Web Server**: Apache, Nginx

### Environment Variables
```bash
# Production environment variables
REACT_APP_API_URL=https://api.your-domain.com
REACT_APP_API_KEY=your-production-api-key
```

## Development Guidelines

### Code Style
- **TypeScript**: Strict typing enabled
- **ESLint**: Code quality enforcement
- **Prettier**: Code formatting (recommended)

### Component Guidelines
- **Functional Components**: Prefer hooks over class components
- **Props Interface**: Define TypeScript interfaces for all props
- **Error Boundaries**: Implement error handling for robust UX

### File Organization
```
components/
├── ComponentName.tsx      # Component implementation
├── ComponentName.css      # Component styles
└── ComponentName.test.tsx # Component tests
```

## Browser Support

### Supported Browsers
- Chrome (last 2 versions)
- Firefox (last 2 versions)
- Safari (last 2 versions)
- Edge (last 2 versions)

### Polyfills
- **Core JS**: ES6+ feature support
- **React**: Built-in polyfill support

## Future Enhancements

### Planned Features
- **Real-time Updates**: WebSocket integration for live data
- **Advanced Analytics**: Enhanced metrics and reporting
- **User Management**: Multi-user admin access
- **Theme Support**: Dark/light mode toggle
- **Internationalization**: Multi-language support

### Performance Improvements
- **Virtual Scrolling**: For large message lists
- **Progressive Loading**: Lazy load components and data
- **Service Workers**: Offline capability and caching