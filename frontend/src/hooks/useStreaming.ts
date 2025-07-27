import { useState, useEffect, useCallback, useRef } from 'react';
import { 
  StreamingService, 
  StreamingEvent, 
  StreamingEventType, 
  SubscribeRequest,
  streamingService 
} from '../services/streamingService';
import { CONFIG } from '../utils/config';

export interface UseStreamingOptions {
  autoConnect?: boolean;
  apiKey?: string;
}

export interface StreamingState {
  connected: boolean;
  connecting: boolean;
  error: string | null;
  subscriptions: string[];
}

export interface UseStreamingReturn {
  state: StreamingState;
  events: StreamingEvent[];
  connect: () => Promise<void>;
  disconnect: () => void;
  subscribe: (request: SubscribeRequest) => Promise<string>;
  unsubscribe: (subscriptionId: string) => Promise<void>;
  clearEvents: () => void;
  getEventsByType: (eventType: StreamingEventType) => StreamingEvent[];
  getEventsByQueue: (queueName: string) => StreamingEvent[];
}

export function useStreaming(options: UseStreamingOptions = {}): UseStreamingReturn {
  const {
    autoConnect = true,
    apiKey = CONFIG.API_KEY
  } = options;

  const [state, setState] = useState<StreamingState>({
    connected: false,
    connecting: false,
    error: null,
    subscriptions: []
  });

  const [events, setEvents] = useState<StreamingEvent[]>([]);
  const serviceRef = useRef<StreamingService>(streamingService);
  const maxEvents = 1000; // 최대 이벤트 저장 수

  // 연결 상태 업데이트
  const updateConnectionStatus = useCallback((connected: boolean) => {
    setState(prev => ({
      ...prev,
      connected,
      connecting: false,
      error: connected ? null : prev.error
    }));
  }, []);

  // 이벤트 핸들러
  const handleEvent = useCallback((event: StreamingEvent) => {
    setEvents(prev => {
      const newEvents = [event, ...prev];
      // 최대 이벤트 수 제한
      return newEvents.slice(0, maxEvents);
    });
  }, [maxEvents]);

  // 연결 함수
  const connect = useCallback(async () => {
    if (state.connected || state.connecting) {
      return;
    }

    setState(prev => ({ ...prev, connecting: true, error: null }));

    try {
      await serviceRef.current.connect();
    } catch (error) {
      setState(prev => ({
        ...prev,
        connecting: false,
        error: error instanceof Error ? error.message : 'Connection failed'
      }));
      throw error;
    }
  }, [state.connected, state.connecting]);

  // 연결 해제 함수
  const disconnect = useCallback(() => {
    serviceRef.current.disconnect();
    setState(prev => ({
      ...prev,
      connected: false,
      connecting: false,
      subscriptions: []
    }));
  }, []);

  // 구독 함수
  const subscribe = useCallback(async (request: SubscribeRequest): Promise<string> => {
    if (!state.connected) {
      throw new Error('Not connected to streaming service');
    }

    try {
      const subscriptionId = await serviceRef.current.subscribe(request);
      setState(prev => ({
        ...prev,
        subscriptions: [...prev.subscriptions, subscriptionId]
      }));
      return subscriptionId;
    } catch (error) {
      setState(prev => ({
        ...prev,
        error: error instanceof Error ? error.message : 'Subscription failed'
      }));
      throw error;
    }
  }, [state.connected]);

  // 구독 해제 함수
  const unsubscribe = useCallback(async (subscriptionId: string): Promise<void> => {
    if (!state.connected) {
      throw new Error('Not connected to streaming service');
    }

    try {
      await serviceRef.current.unsubscribe(subscriptionId);
      setState(prev => ({
        ...prev,
        subscriptions: prev.subscriptions.filter(id => id !== subscriptionId)
      }));
    } catch (error) {
      setState(prev => ({
        ...prev,
        error: error instanceof Error ? error.message : 'Unsubscribe failed'
      }));
      throw error;
    }
  }, [state.connected]);

  // 이벤트 초기화
  const clearEvents = useCallback(() => {
    setEvents([]);
  }, []);

  // 타입별 이벤트 필터링
  const getEventsByType = useCallback((eventType: StreamingEventType): StreamingEvent[] => {
    return events.filter(event => event.eventType === eventType);
  }, [events]);

  // 큐별 이벤트 필터링
  const getEventsByQueue = useCallback((queueName: string): StreamingEvent[] => {
    return events.filter(event => event.queueName === queueName);
  }, [events]);

  // 컴포넌트 마운트 시 이벤트 리스너 등록
  useEffect(() => {
    const service = serviceRef.current;

    // 이벤트 핸들러 등록
    const unsubscribeEvent = service.onEvent(handleEvent);
    const unsubscribeConnection = service.onConnectionStatus(updateConnectionStatus);

    // 자동 연결
    if (autoConnect && CONFIG.FEATURES.enableStreaming) {
      connect().catch(error => {
        console.error('Auto-connect failed:', error);
      });
    }

    return () => {
      unsubscribeEvent();
      unsubscribeConnection();
    };
  }, [autoConnect, connect, handleEvent, updateConnectionStatus]);

  // 컴포넌트 언마운트 시 정리
  useEffect(() => {
    return () => {
      if (state.connected) {
        serviceRef.current.disconnect();
      }
    };
  }, []);

  return {
    state,
    events,
    connect,
    disconnect,
    subscribe,
    unsubscribe,
    clearEvents,
    getEventsByType,
    getEventsByQueue
  };
}

export default useStreaming;