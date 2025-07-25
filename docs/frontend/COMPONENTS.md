# Component Reference - Frontend

## Component Hierarchy

```
App.tsx
├── Navigation.tsx
└── Routes
    ├── Dashboard.tsx
    ├── QueueManagement.tsx
    │   ├── QueueManager.tsx
    │   ├── MessageManager.tsx
    │   ├── EnhancedMessageManager.tsx
    │   └── AdvancedMessageSearch.tsx
    ├── TenantManagement.tsx
    └── SystemMonitoring.tsx
        ├── StorageTierManager.tsx
        └── IntelligentBatchProcessor.tsx
```

## Core Components

### App.tsx
**Purpose**: Root application component with routing setup

**Features**:
- React Router configuration
- Main layout structure
- Global navigation integration

**Routes**:
- `/` → Redirect to `/dashboard`
- `/dashboard` → Dashboard component
- `/queues` → QueueManagement component
- `/tenants` → TenantManagement component
- `/monitoring` → SystemMonitoring component

**CSS**: `App.css`

---

### Navigation.tsx
**Purpose**: Main navigation menu component

**Features**:
- Responsive navigation bar
- Active route highlighting
- Mobile-friendly hamburger menu (if implemented)

**Props**: None

**State**:
```typescript
// No local state - uses React Router for active state
```

**CSS**: `Navigation.css`

**Key Methods**:
- Navigation handled by React Router Link components

---

## Page Components

### Dashboard.tsx
**Purpose**: System overview and main dashboard

**Features**:
- System statistics display
- Recent activity feed
- Quick action buttons
- Performance metrics charts

**Props**: None

**State**:
```typescript
interface DashboardState {
  systemStats: SystemStats | null;
  recentActivity: Activity[];
  loading: boolean;
  error: string | null;
}
```

**API Calls**:
- `GET /api/admin/dashboard` - System overview data
- `GET /api/admin/activity` - Recent activity

**Key Methods**:
- `fetchDashboardData()`: Load dashboard statistics
- `refreshData()`: Manual data refresh
- `handleQuickAction()`: Execute dashboard quick actions

**CSS**: `Dashboard.css`

---

### QueueManagement.tsx
**Purpose**: Main queue management interface

**Features**:
- Queue list display
- Queue creation/deletion
- Queue statistics
- Integration with child components

**Props**: None

**State**:
```typescript
interface QueueManagementState {
  queues: Queue[];
  selectedQueue: string | null;
  showCreateForm: boolean;
  loading: boolean;
  error: string | null;
}
```

**Child Components**:
- `QueueManager`: Queue CRUD operations
- `MessageManager`: Message operations
- `EnhancedMessageManager`: Advanced message features
- `AdvancedMessageSearch`: Message search functionality

**API Calls**:
- `GET /api/queues` - List all queues
- `POST /api/queues` - Create new queue
- `DELETE /api/queues/{name}` - Delete queue

**Key Methods**:
- `loadQueues()`: Fetch queue list
- `createQueue()`: Create new queue
- `deleteQueue()`: Remove queue
- `selectQueue()`: Set active queue

**CSS**: `QueueManagement.css`

---

### TenantManagement.tsx
**Purpose**: Multi-tenant administration interface

**Features**:
- Tenant list display
- Tenant creation/modification
- API key management
- Usage statistics per tenant

**Props**: None

**State**:
```typescript
interface TenantManagementState {
  tenants: TenantInfo[];
  selectedTenant: TenantInfo | null;
  showCreateForm: boolean;
  loading: boolean;
  error: string | null;
}
```

**API Calls**:
- `GET /api/admin/tenants` - List tenants
- `POST /api/admin/tenants` - Create tenant
- `PUT /api/admin/tenants/{id}` - Update tenant
- `DELETE /api/admin/tenants/{id}` - Delete tenant

**Key Methods**:
- `loadTenants()`: Fetch tenant list
- `createTenant()`: Create new tenant
- `updateTenant()`: Modify tenant
- `generateApiKey()`: Generate new API key
- `suspendTenant()`: Suspend tenant access

**CSS**: `TenantManagement.css`

---

### SystemMonitoring.tsx
**Purpose**: System health and performance monitoring

**Features**:
- Real-time system metrics
- Performance charts
- Alert management
- Storage tier monitoring

**Props**: None

**State**:
```typescript
interface SystemMonitoringState {
  metrics: SystemMetrics;
  alerts: Alert[];
  chartData: ChartData[];
  refreshInterval: number;
  autoRefresh: boolean;
}
```

**Child Components**:
- `StorageTierManager`: Storage performance monitoring
- `IntelligentBatchProcessor`: Batch processing monitoring

**API Calls**:
- `GET /api/admin/metrics` - System metrics
- `GET /api/admin/alerts` - Active alerts
- `GET /api/storage/stats` - Storage statistics

**Key Methods**:
- `fetchMetrics()`: Load system metrics
- `startAutoRefresh()`: Enable automatic data refresh
- `stopAutoRefresh()`: Disable automatic refresh
- `acknowledgeAlert()`: Mark alert as acknowledged

**CSS**: `SystemMonitoring.css`

---

## Feature Components

### QueueManager.tsx
**Purpose**: Queue-specific CRUD operations

**Props**:
```typescript
interface QueueManagerProps {
  onQueueCreated: (queue: Queue) => void;
  onQueueDeleted: (queueName: string) => void;
  onQueueSelected: (queueName: string) => void;
}
```

**Features**:
- Queue creation form
- Queue configuration
- Queue attribute management
- Queue statistics display

**State**:
```typescript
interface QueueManagerState {
  newQueue: CreateQueueRequest;
  showAdvanced: boolean;
  validation: ValidationErrors;
}
```

**Key Methods**:
- `validateQueueName()`: Validate queue name format
- `submitQueue()`: Create new queue
- `configureAttributes()`: Set queue attributes

**CSS**: `QueueManager.css`

---

### MessageManager.tsx
**Purpose**: Core message operations within a queue

**Props**:
```typescript
interface MessageManagerProps {
  queueName: string;
}
```

**Features**:
- Send single messages
- Send batch messages (up to 10)
- Receive messages
- Delete messages (single/batch)
- Purge queue
- Message attribute management

**State**:
```typescript
interface MessageManagerState {
  messages: MessageResponse[];
  selectedMessages: string[];
  singleMessage: SendMessageRequest;
  batchMessages: BatchMessageRequest[];
  loading: boolean;
  message: NotificationMessage | null;
}
```

**Key Methods**:
- `sendSingleMessage()`: Send one message
- `sendBatchMessages()`: Send multiple messages
- `receiveMessages()`: Fetch messages from queue
- `deleteMessage()`: Remove single message
- `deleteSelectedMessages()`: Bulk delete messages
- `purgeQueue()`: Clear all messages

**CSS**: `MessageManager.css`

---

### EnhancedMessageManager.tsx
**Purpose**: Advanced message management features

**Props**:
```typescript
interface EnhancedMessageManagerProps {
  queueName: string;
  onMessageAction: (action: string, messageId: string) => void;
}
```

**Features**:
- Advanced message filtering
- Message preview with metadata
- Bulk operations with progress tracking
- Message export functionality
- Message history tracking

**State**:
```typescript
interface EnhancedMessageManagerState {
  messages: EnhancedMessage[];
  filters: MessageFilters;
  selectedMessages: string[];
  bulkOperation: BulkOperation | null;
  previewMessage: MessageResponse | null;
}
```

**Key Methods**:
- `applyFilters()`: Filter messages by criteria
- `previewMessage()`: Show message details
- `exportMessages()`: Export filtered messages
- `executeBulkOperation()`: Perform bulk actions

**CSS**: `EnhancedMessageManager.css`

---

### AdvancedMessageSearch.tsx
**Purpose**: Complex message search and filtering

**Props**:
```typescript
interface AdvancedMessageSearchProps {
  queueName?: string;
  onSearchResults: (results: MessageSearchResult[]) => void;
}
```

**Features**:
- Multi-criteria search
- Date range filtering
- Attribute-based filtering
- Cross-tier search (Hot/Warm/Cold)
- Search result export

**State**:
```typescript
interface AdvancedMessageSearchState {
  searchCriteria: SearchCriteria;
  searchResults: MessageSearchResult[];
  searching: boolean;
  resultCount: number;
  exportFormat: 'JSON' | 'CSV' | 'XML';
}
```

**Key Methods**:
- `buildSearchQuery()`: Construct search parameters
- `executeSearch()`: Perform search across tiers
- `exportResults()`: Export search results
- `saveSearchCriteria()`: Save search for reuse

**CSS**: `AdvancedMessageSearch.css`

---

### StorageTierManager.tsx
**Purpose**: Storage tier monitoring and management

**Props**:
```typescript
interface StorageTierManagerProps {
  refreshInterval?: number;
}
```

**Features**:
- Tier statistics visualization
- Message distribution charts
- Manual tier promotion/demotion
- Storage performance metrics
- Tier health monitoring

**State**:
```typescript
interface StorageTierManagerState {
  tierStats: TierStats;
  chartData: TierChartData[];
  selectedTier: 'HOT' | 'WARM' | 'COLD';
  promotionQueue: PromotionRequest[];
}
```

**Key Methods**:
- `fetchTierStats()`: Load storage statistics
- `promoteMessage()`: Move message to higher tier
- `demoteMessage()`: Move message to lower tier
- `optimizeTiers()`: Trigger tier optimization

**CSS**: `StorageTierManager.css`

---

### IntelligentBatchProcessor.tsx
**Purpose**: Smart batch processing with optimization

**Props**:
```typescript
interface IntelligentBatchProcessorProps {
  queueName: string;
  onBatchComplete: (result: BatchResult) => void;
}
```

**Features**:
- Automated batch optimization
- Progress tracking with real-time updates
- Error handling and retry logic
- Performance metrics and analysis
- Batch size recommendations

**State**:
```typescript
interface IntelligentBatchProcessorState {
  batchOperations: BatchOperation[];
  currentBatch: BatchOperation | null;
  progress: BatchProgress;
  optimization: BatchOptimization;
  metrics: BatchMetrics;
}
```

**Key Methods**:
- `optimizeBatchSize()`: Calculate optimal batch size
- `processBatch()`: Execute batch operation
- `handleBatchError()`: Error recovery logic
- `updateProgress()`: Real-time progress updates

**CSS**: `IntelligentBatchProcessor.css`

---

## Component Patterns

### Error Handling Pattern
```typescript
const [error, setError] = useState<string | null>(null);

const handleApiError = (error: any) => {
  const message = error.response?.data?.error?.message || 'An error occurred';
  setError(message);
  setTimeout(() => setError(null), 5000); // Auto-clear after 5 seconds
};
```

### Loading State Pattern
```typescript
const [loading, setLoading] = useState(false);

const fetchData = async () => {
  setLoading(true);
  try {
    const result = await apiCall();
    setData(result);
  } catch (error) {
    handleError(error);
  } finally {
    setLoading(false);
  }
};
```

### Form Validation Pattern
```typescript
interface ValidationErrors {
  [field: string]: string;
}

const validateForm = (data: FormData): ValidationErrors => {
  const errors: ValidationErrors = {};
  
  if (!data.required_field) {
    errors.required_field = 'This field is required';
  }
  
  return errors;
};
```

### Message Notification Pattern
```typescript
interface NotificationMessage {
  type: 'success' | 'error' | 'warning' | 'info';
  text: string;
}

const [message, setMessage] = useState<NotificationMessage | null>(null);

// Auto-clear messages after 3 seconds
useEffect(() => {
  if (message) {
    const timer = setTimeout(() => setMessage(null), 3000);
    return () => clearTimeout(timer);
  }
}, [message]);
```

## Styling Guidelines

### CSS Class Naming Convention
```css
/* Component-based naming */
.queue-management { /* Main component container */ }
.queue-management__header { /* Component section */ }
.queue-management__button { /* Component element */ }
.queue-management__button--primary { /* Element modifier */ }
```

### Responsive Design Pattern
```css
/* Mobile-first responsive design */
.component {
  /* Mobile styles */
}

@media (min-width: 768px) {
  .component {
    /* Tablet styles */
  }
}

@media (min-width: 1024px) {
  .component {
    /* Desktop styles */
  }
}
```

### State-based Styling
```css
.component {
  opacity: 1;
  transition: opacity 0.3s ease;
}

.component--loading {
  opacity: 0.6;
  pointer-events: none;
}

.component--error {
  border-color: #ff6b6b;
  background-color: #ffe6e6;
}
```

## Testing Components

### Component Test Structure
```typescript
// ComponentName.test.tsx
import { render, screen, fireEvent } from '@testing-library/react';
import ComponentName from './ComponentName';

describe('ComponentName', () => {
  it('renders correctly', () => {
    render(<ComponentName />);
    expect(screen.getByText('Expected Text')).toBeInTheDocument();
  });

  it('handles user interaction', () => {
    const mockHandler = jest.fn();
    render(<ComponentName onAction={mockHandler} />);
    
    fireEvent.click(screen.getByRole('button'));
    expect(mockHandler).toHaveBeenCalled();
  });
});
```