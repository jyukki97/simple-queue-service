import { CONFIG } from '../utils/config';

export interface StreamingMessage {
  messageId: string;
  tenantId: string;
  queueName: string;
  messageBody: string;
  attributes: Record<string, string>;
  timestamp: string;
  messageGroupId?: string;
  messageType: string;
  sequenceNumber?: number;
}

export interface StreamingEvent {
  eventId: string;
  eventType: StreamingEventType;
  tenantId: string;
  queueName: string;
  timestamp: string;
  data: any;
}

export enum StreamingEventType {
  MESSAGE_SENT = 'MESSAGE_SENT',
  MESSAGE_RECEIVED = 'MESSAGE_RECEIVED',
  MESSAGE_DELETED = 'MESSAGE_DELETED',
  QUEUE_CREATED = 'QUEUE_CREATED',
  QUEUE_DELETED = 'QUEUE_DELETED',
  QUEUE_PURGED = 'QUEUE_PURGED',
  CONNECTION_ESTABLISHED = 'CONNECTION_ESTABLISHED',
  CONNECTION_LOST = 'CONNECTION_LOST',
  HEARTBEAT = 'HEARTBEAT'
}

export interface SubscribeRequest {
  queueNames?: string[];
  eventTypes?: StreamingEventType[];
  filterExpression?: string;
  batchSize?: number;
  maxWaitTime?: number;
}

export interface StreamingResponse {
  type: string;
  success: boolean;
  data?: any;
  error?: string;
  timestamp: string;
}

export type StreamingEventHandler = (event: StreamingEvent) => void;
export type StreamingResponseHandler = (response: StreamingResponse) => void;
export type ConnectionStatusHandler = (connected: boolean) => void;

export class StreamingService {
  private ws: WebSocket | null = null;
  private eventHandlers: StreamingEventHandler[] = [];
  private responseHandlers: StreamingResponseHandler[] = [];
  private connectionHandlers: ConnectionStatusHandler[] = [];
  private reconnectAttempts = 0;
  private maxReconnectAttempts = 5;
  private reconnectDelay = 1000;
  private apiKey: string;
  private isConnecting = false;

  constructor(apiKey: string = 'test-api-key') {
    this.apiKey = apiKey;
  }

  /**
   * WebSocket 연결을 시작합니다
   */
  connect(): Promise<void> {
    if (this.ws?.readyState === WebSocket.OPEN) {
      return Promise.resolve();
    }

    if (this.isConnecting) {
      return Promise.reject(new Error('Already connecting'));
    }

    this.isConnecting = true;

    return new Promise((resolve, reject) => {
      try {
        const wsUrl = `${CONFIG.WS_BASE_URL}/ws/stream?apiKey=${this.apiKey}`;
        console.log('Connecting to WebSocket:', wsUrl);
        
        this.ws = new WebSocket(wsUrl);

        this.ws.onopen = () => {
          console.log('WebSocket connected');
          this.isConnecting = false;
          this.reconnectAttempts = 0;
          this.notifyConnectionHandlers(true);
          resolve();
        };

        this.ws.onmessage = (event) => {
          try {
            const message = JSON.parse(event.data);
            this.handleMessage(message);
          } catch (error) {
            console.error('Error parsing WebSocket message:', error);
          }
        };

        this.ws.onclose = (event) => {
          console.log('WebSocket closed:', event.code, event.reason);
          this.isConnecting = false;
          this.ws = null;
          this.notifyConnectionHandlers(false);

          // 자동 재연결 시도
          if (this.reconnectAttempts < this.maxReconnectAttempts) {
            this.scheduleReconnect();
          }
        };

        this.ws.onerror = (error) => {
          console.error('WebSocket error:', error);
          this.isConnecting = false;
          this.notifyConnectionHandlers(false);
          reject(error);
        };

        // 연결 타임아웃
        setTimeout(() => {
          if (this.isConnecting) {
            this.isConnecting = false;
            this.ws?.close();
            reject(new Error('Connection timeout'));
          }
        }, 10000);

      } catch (error) {
        this.isConnecting = false;
        reject(error);
      }
    });
  }

  /**
   * WebSocket 연결을 종료합니다
   */
  disconnect(): void {
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    this.reconnectAttempts = this.maxReconnectAttempts; // 재연결 방지
  }

  /**
   * 큐에 구독합니다
   */
  subscribe(request: SubscribeRequest): Promise<string> {
    return this.sendMessage({
      type: 'SUBSCRIBE',
      data: {
        queueNames: request.queueNames || [],
        eventTypes: request.eventTypes || [StreamingEventType.MESSAGE_SENT],
        filterExpression: request.filterExpression,
        batchSize: request.batchSize || 1,
        maxWaitTime: request.maxWaitTime || 10000
      }
    }).then(response => {
      if (response.success && response.data?.subscriptionId) {
        return response.data.subscriptionId;
      }
      throw new Error(response.error || 'Subscription failed');
    });
  }

  /**
   * 구독을 취소합니다
   */
  unsubscribe(subscriptionId: string): Promise<void> {
    return this.sendMessage({
      type: 'UNSUBSCRIBE',
      data: { subscriptionId }
    }).then(response => {
      if (!response.success) {
        throw new Error(response.error || 'Unsubscribe failed');
      }
    });
  }

  /**
   * 핑을 전송합니다
   */
  ping(): Promise<void> {
    return this.sendMessage({
      type: 'PING'
    }).then(response => {
      if (!response.success) {
        throw new Error(response.error || 'Ping failed');
      }
    });
  }

  /**
   * 이벤트 핸들러를 등록합니다
   */
  onEvent(handler: StreamingEventHandler): () => void {
    this.eventHandlers.push(handler);
    return () => {
      const index = this.eventHandlers.indexOf(handler);
      if (index > -1) {
        this.eventHandlers.splice(index, 1);
      }
    };
  }

  /**
   * 응답 핸들러를 등록합니다
   */
  onResponse(handler: StreamingResponseHandler): () => void {
    this.responseHandlers.push(handler);
    return () => {
      const index = this.responseHandlers.indexOf(handler);
      if (index > -1) {
        this.responseHandlers.splice(index, 1);
      }
    };
  }

  /**
   * 연결 상태 핸들러를 등록합니다
   */
  onConnectionStatus(handler: ConnectionStatusHandler): () => void {
    this.connectionHandlers.push(handler);
    return () => {
      const index = this.connectionHandlers.indexOf(handler);
      if (index > -1) {
        this.connectionHandlers.splice(index, 1);
      }
    };
  }

  /**
   * 연결 상태를 반환합니다
   */
  isConnected(): boolean {
    return this.ws?.readyState === WebSocket.OPEN;
  }

  // ========== 내부 메서드들 ==========

  private sendMessage(message: any): Promise<StreamingResponse> {
    return new Promise((resolve, reject) => {
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
        reject(new Error('WebSocket not connected'));
        return;
      }

      // 응답 대기를 위한 임시 핸들러
      const responseHandler = (response: StreamingResponse) => {
        if (response.type === `${message.type}_SUCCESS` || 
            response.type === `${message.type}_ERROR` ||
            response.type === 'PONG' && message.type === 'PING') {
          resolve(response);
          // 임시 핸들러 제거
          const index = this.responseHandlers.indexOf(responseHandler);
          if (index > -1) {
            this.responseHandlers.splice(index, 1);
          }
        }
      };

      this.responseHandlers.push(responseHandler);

      try {
        this.ws.send(JSON.stringify(message));
        
        // 타임아웃 설정
        setTimeout(() => {
          const index = this.responseHandlers.indexOf(responseHandler);
          if (index > -1) {
            this.responseHandlers.splice(index, 1);
            reject(new Error('Response timeout'));
          }
        }, 10000);

      } catch (error) {
        const index = this.responseHandlers.indexOf(responseHandler);
        if (index > -1) {
          this.responseHandlers.splice(index, 1);
        }
        reject(error);
      }
    });
  }

  private handleMessage(message: any): void {
    try {
      // 스트리밍 이벤트인지 응답인지 구분
      if (message.eventType && message.eventId) {
        // 스트리밍 이벤트
        this.notifyEventHandlers(message as StreamingEvent);
      } else if (message.type && typeof message.success === 'boolean') {
        // 응답 메시지
        this.notifyResponseHandlers(message as StreamingResponse);
      } else {
        console.warn('Unknown message format:', message);
      }
    } catch (error) {
      console.error('Error handling WebSocket message:', error);
    }
  }

  private notifyEventHandlers(event: StreamingEvent): void {
    this.eventHandlers.forEach(handler => {
      try {
        handler(event);
      } catch (error) {
        console.error('Error in event handler:', error);
      }
    });
  }

  private notifyResponseHandlers(response: StreamingResponse): void {
    this.responseHandlers.forEach(handler => {
      try {
        handler(response);
      } catch (error) {
        console.error('Error in response handler:', error);
      }
    });
  }

  private notifyConnectionHandlers(connected: boolean): void {
    this.connectionHandlers.forEach(handler => {
      try {
        handler(connected);
      } catch (error) {
        console.error('Error in connection handler:', error);
      }
    });
  }

  private scheduleReconnect(): void {
    this.reconnectAttempts++;
    const delay = this.reconnectDelay * Math.pow(2, this.reconnectAttempts - 1);
    
    console.log(`Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts}/${this.maxReconnectAttempts})`);
    
    setTimeout(() => {
      this.connect().catch(error => {
        console.error('Reconnection failed:', error);
      });
    }, delay);
  }
}

// 싱글톤 인스턴스
export const streamingService = new StreamingService();