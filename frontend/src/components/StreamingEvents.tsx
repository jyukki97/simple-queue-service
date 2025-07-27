import React, { useState, useMemo } from 'react';
import { StreamingEvent, StreamingEventType } from '../services/streamingService';
import useStreaming from '../hooks/useStreaming';
import './StreamingEvents.css';

interface StreamingEventsProps {
  queueFilter?: string;
  eventTypeFilter?: StreamingEventType;
  maxEvents?: number;
}

const StreamingEvents: React.FC<StreamingEventsProps> = ({
  queueFilter,
  eventTypeFilter,
  maxEvents = 100
}) => {
  const { state, events, connect, disconnect, subscribe, clearEvents } = useStreaming();
  const [subscriptionId, setSubscriptionId] = useState<string | null>(null);
  const [selectedQueues, setSelectedQueues] = useState<string[]>(['']);
  const [selectedEventTypes, setSelectedEventTypes] = useState<StreamingEventType[]>([
    StreamingEventType.MESSAGE_SENT
  ]);

  // 이벤트 필터링
  const filteredEvents = useMemo(() => {
    let filtered = events;

    if (queueFilter) {
      filtered = filtered.filter(event => event.queueName === queueFilter);
    }

    if (eventTypeFilter) {
      filtered = filtered.filter(event => event.eventType === eventTypeFilter);
    }

    return filtered.slice(0, maxEvents);
  }, [events, queueFilter, eventTypeFilter, maxEvents]);

  // 구독 시작
  const handleSubscribe = async () => {
    if (!state.connected) {
      await connect();
    }

    try {
      const id = await subscribe({
        queueNames: selectedQueues.filter(q => q.trim() !== ''),
        eventTypes: selectedEventTypes
      });
      setSubscriptionId(id);
    } catch (error) {
      console.error('Subscription failed:', error);
    }
  };

  // 이벤트 타입 색상 매핑
  const getEventTypeColor = (eventType: StreamingEventType): string => {
    switch (eventType) {
      case StreamingEventType.MESSAGE_SENT:
        return '#10b981'; // green
      case StreamingEventType.MESSAGE_RECEIVED:
        return '#3b82f6'; // blue
      case StreamingEventType.MESSAGE_DELETED:
        return '#ef4444'; // red
      case StreamingEventType.QUEUE_CREATED:
        return '#8b5cf6'; // purple
      case StreamingEventType.QUEUE_DELETED:
        return '#f59e0b'; // amber
      case StreamingEventType.HEARTBEAT:
        return '#6b7280'; // gray
      default:
        return '#374151'; // gray-700
    }
  };

  // 이벤트 데이터 포맷팅
  const formatEventData = (event: StreamingEvent): string => {
    if (event.eventType === StreamingEventType.MESSAGE_SENT) {
      const message = event.data;
      return `Message: ${message.messageId}`;
    }
    if (event.eventType === StreamingEventType.MESSAGE_RECEIVED) {
      return `Receipt: ${event.data.receiptHandle}`;
    }
    if (event.eventType === StreamingEventType.MESSAGE_DELETED) {
      return `Deleted: ${event.data.messageId}`;
    }
    return JSON.stringify(event.data, null, 2);
  };

  return (
    <div className="streaming-events">
      <div className="streaming-header">
        <h3>실시간 이벤트 스트림</h3>
        <div className="connection-status">
          <span className={`status-indicator ${state.connected ? 'connected' : 'disconnected'}`}>
            {state.connecting ? '연결 중...' : state.connected ? '연결됨' : '연결 끊김'}
          </span>
        </div>
      </div>

      {state.error && (
        <div className="error-message">
          오류: {state.error}
        </div>
      )}

      <div className="subscription-controls">
        <div className="control-group">
          <label>큐 이름 (쉼표로 구분):</label>
          <input
            type="text"
            value={selectedQueues.join(', ')}
            onChange={(e) => setSelectedQueues(e.target.value.split(',').map(q => q.trim()))}
            placeholder="큐 이름을 입력하세요 (빈 값이면 모든 큐)"
          />
        </div>

        <div className="control-group">
          <label>이벤트 타입:</label>
          <select
            multiple
            value={selectedEventTypes}
            onChange={(e) => {
              const values = Array.from(e.target.selectedOptions, option => option.value as StreamingEventType);
              setSelectedEventTypes(values);
            }}
          >
            {Object.values(StreamingEventType).map(type => (
              <option key={type} value={type}>
                {type.replace('_', ' ')}
              </option>
            ))}
          </select>
        </div>

        <div className="action-buttons">
          {!subscriptionId ? (
            <button 
              onClick={handleSubscribe}
              disabled={state.connecting}
              className="subscribe-button"
            >
              구독 시작
            </button>
          ) : (
            <button 
              onClick={() => setSubscriptionId(null)}
              className="unsubscribe-button"
            >
              구독 중지
            </button>
          )}
          
          <button onClick={clearEvents} className="clear-button">
            이벤트 초기화
          </button>
          
          {!state.connected && (
            <button onClick={connect} disabled={state.connecting} className="connect-button">
              연결
            </button>
          )}
          
          {state.connected && (
            <button onClick={disconnect} className="disconnect-button">
              연결 해제
            </button>
          )}
        </div>
      </div>

      <div className="events-container">
        <div className="events-header">
          <span>이벤트 ({filteredEvents.length})</span>
        </div>
        
        <div className="events-list">
          {filteredEvents.length === 0 ? (
            <div className="no-events">
              이벤트가 없습니다.
            </div>
          ) : (
            filteredEvents.map((event, index) => (
              <div key={`${event.eventId}-${index}`} className="event-item">
                <div className="event-header">
                  <span 
                    className="event-type"
                    style={{ backgroundColor: getEventTypeColor(event.eventType) }}
                  >
                    {event.eventType}
                  </span>
                  <span className="event-queue">{event.queueName}</span>
                  <span className="event-time">
                    {new Date(event.timestamp).toLocaleTimeString()}
                  </span>
                </div>
                <div className="event-data">
                  {formatEventData(event)}
                </div>
              </div>
            ))
          )}
        </div>
      </div>
    </div>
  );
};

export default StreamingEvents;