import React, { useState, useEffect } from 'react';
import './QueueManagement.css';

interface Queue {
  queueName: string;
  tenantId: string;
  attributes: {
    visibilityTimeoutSeconds: number;
    messageRetentionPeriod: number;
    delaySeconds: number;
    maxReceiveCount: number;
    deadLetterTargetArn?: string;
    fifoQueue: boolean;
  };
  createdAt: string;
  messageCount: number;
  messagesInFlight: number;
}

const QueueManagement: React.FC = () => {
  const [queues, setQueues] = useState<Queue[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [newQueue, setNewQueue] = useState({
    queueName: '',
    tenantId: 'tenant-1', // Default tenant
    visibilityTimeoutSeconds: 30,
    messageRetentionPeriod: 1209600,
    delaySeconds: 0,
    maxReceiveCount: 3,
    fifoQueue: false
  });

  useEffect(() => {
    fetchQueues();
  }, []);

  const fetchQueues = async () => {
    try {
      setLoading(true);
      // Mock data - 실제로는 API에서 가져와야 함
      const mockQueues: Queue[] = [
        {
          queueName: 'orders-queue',
          tenantId: 'tenant-1',
          attributes: {
            visibilityTimeoutSeconds: 30,
            messageRetentionPeriod: 1209600,
            delaySeconds: 0,
            maxReceiveCount: 3,
            fifoQueue: false
          },
          createdAt: '2024-01-15T10:30:00Z',
          messageCount: 1250,
          messagesInFlight: 25
        },
        {
          queueName: 'notifications-queue',
          tenantId: 'tenant-2',
          attributes: {
            visibilityTimeoutSeconds: 60,
            messageRetentionPeriod: 604800,
            delaySeconds: 5,
            maxReceiveCount: 5,
            fifoQueue: false
          },
          createdAt: '2024-01-10T08:15:00Z',
          messageCount: 890,
          messagesInFlight: 12
        },
        {
          queueName: 'analytics-queue.fifo',
          tenantId: 'tenant-1',
          attributes: {
            visibilityTimeoutSeconds: 45,
            messageRetentionPeriod: 1209600,
            delaySeconds: 0,
            maxReceiveCount: 3,
            fifoQueue: true
          },
          createdAt: '2024-01-12T14:20:00Z',
          messageCount: 650,
          messagesInFlight: 8
        }
      ];
      
      setQueues(mockQueues);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : '큐 목록을 불러오는데 실패했습니다.');
    } finally {
      setLoading(false);
    }
  };

  const handleCreateQueue = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      // Mock creation - 실제로는 API 호출
      const createdQueue: Queue = {
        ...newQueue,
        createdAt: new Date().toISOString(),
        messageCount: 0,
        messagesInFlight: 0,
        attributes: {
          visibilityTimeoutSeconds: newQueue.visibilityTimeoutSeconds,
          messageRetentionPeriod: newQueue.messageRetentionPeriod,
          delaySeconds: newQueue.delaySeconds,
          maxReceiveCount: newQueue.maxReceiveCount,
          fifoQueue: newQueue.fifoQueue
        }
      };
      
      setQueues([...queues, createdQueue]);
      setShowCreateForm(false);
      setNewQueue({
        queueName: '',
        tenantId: 'tenant-1',
        visibilityTimeoutSeconds: 30,
        messageRetentionPeriod: 1209600,
        delaySeconds: 0,
        maxReceiveCount: 3,
        fifoQueue: false
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : '큐 생성에 실패했습니다.');
    }
  };

  const handleDeleteQueue = async (queueName: string) => {
    if (!window.confirm(`정말로 "${queueName}" 큐를 삭제하시겠습니까?`)) {
      return;
    }
    
    try {
      // Mock deletion - 실제로는 API 호출
      setQueues(queues.filter(q => q.queueName !== queueName));
    } catch (err) {
      setError(err instanceof Error ? err.message : '큐 삭제에 실패했습니다.');
    }
  };

  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleDateString('ko-KR', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    });
  };

  const formatNumber = (num: number) => {
    return new Intl.NumberFormat().format(num);
  };

  if (loading) {
    return (
      <div className="queue-management loading">
        <div className="spinner"></div>
        <p>큐 목록을 불러오는 중...</p>
      </div>
    );
  }

  return (
    <div className="queue-management">
      <div className="queue-header">
        <h1>📦 큐 관리</h1>
        <button 
          className="create-btn"
          onClick={() => setShowCreateForm(true)}
        >
          ➕ 새 큐 생성
        </button>
      </div>

      {error && (
        <div className="error-message">
          ❌ {error}
        </div>
      )}

      {showCreateForm && (
        <div className="modal-overlay">
          <div className="modal">
            <h2>새 큐 생성</h2>
            <form onSubmit={handleCreateQueue}>
              <div className="form-group">
                <label>큐 이름:</label>
                <input
                  type="text"
                  value={newQueue.queueName}
                  onChange={(e) => setNewQueue({...newQueue, queueName: e.target.value})}
                  placeholder="큐 이름을 입력하세요"
                  required
                />
              </div>

              <div className="form-group">
                <label>테넌트 ID:</label>
                <select
                  value={newQueue.tenantId}
                  onChange={(e) => setNewQueue({...newQueue, tenantId: e.target.value})}
                >
                  <option value="tenant-1">tenant-1</option>
                  <option value="tenant-2">tenant-2</option>
                  <option value="tenant-3">tenant-3</option>
                </select>
              </div>

              <div className="form-group">
                <label>가시성 타임아웃 (초):</label>
                <input
                  type="number"
                  value={newQueue.visibilityTimeoutSeconds}
                  onChange={(e) => setNewQueue({...newQueue, visibilityTimeoutSeconds: Number(e.target.value)})}
                  min="0"
                  max="43200"
                />
              </div>

              <div className="form-group">
                <label>메시지 보존 기간 (초):</label>
                <input
                  type="number"
                  value={newQueue.messageRetentionPeriod}
                  onChange={(e) => setNewQueue({...newQueue, messageRetentionPeriod: Number(e.target.value)})}
                  min="60"
                  max="1209600"
                />
              </div>

              <div className="form-group">
                <label>지연 시간 (초):</label>
                <input
                  type="number"
                  value={newQueue.delaySeconds}
                  onChange={(e) => setNewQueue({...newQueue, delaySeconds: Number(e.target.value)})}
                  min="0"
                  max="900"
                />
              </div>

              <div className="form-group">
                <label>최대 수신 횟수:</label>
                <input
                  type="number"
                  value={newQueue.maxReceiveCount}
                  onChange={(e) => setNewQueue({...newQueue, maxReceiveCount: Number(e.target.value)})}
                  min="1"
                  max="1000"
                />
              </div>

              <div className="form-group checkbox-group">
                <label>
                  <input
                    type="checkbox"
                    checked={newQueue.fifoQueue}
                    onChange={(e) => setNewQueue({...newQueue, fifoQueue: e.target.checked})}
                  />
                  FIFO 큐
                </label>
              </div>

              <div className="form-actions">
                <button type="submit" className="submit-btn">생성</button>
                <button 
                  type="button" 
                  className="cancel-btn"
                  onClick={() => setShowCreateForm(false)}
                >
                  취소
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      <div className="queue-grid">
        {queues.map((queue) => (
          <div key={queue.queueName} className="queue-card">
            <div className="queue-card-header">
              <h3>
                {queue.queueName}
                {queue.attributes.fifoQueue && <span className="fifo-badge">FIFO</span>}
              </h3>
              <button 
                className="delete-btn"
                onClick={() => handleDeleteQueue(queue.queueName)}
                title="큐 삭제"
              >
                🗑️
              </button>
            </div>

            <div className="queue-info">
              <div className="info-item">
                <span className="label">테넌트:</span>
                <span className="value">{queue.tenantId}</span>
              </div>
              <div className="info-item">
                <span className="label">생성일:</span>
                <span className="value">{formatDate(queue.createdAt)}</span>
              </div>
              <div className="info-item">
                <span className="label">메시지 수:</span>
                <span className="value">{formatNumber(queue.messageCount)}</span>
              </div>
              <div className="info-item">
                <span className="label">처리 중:</span>
                <span className="value">{formatNumber(queue.messagesInFlight)}</span>
              </div>
            </div>

            <div className="queue-attributes">
              <h4>속성</h4>
              <div className="attributes-grid">
                <div className="attr-item">
                  <span className="attr-label">가시성 타임아웃:</span>
                  <span className="attr-value">{queue.attributes.visibilityTimeoutSeconds}초</span>
                </div>
                <div className="attr-item">
                  <span className="attr-label">보존 기간:</span>
                  <span className="attr-value">{Math.floor(queue.attributes.messageRetentionPeriod / 86400)}일</span>
                </div>
                <div className="attr-item">
                  <span className="attr-label">지연 시간:</span>
                  <span className="attr-value">{queue.attributes.delaySeconds}초</span>
                </div>
                <div className="attr-item">
                  <span className="attr-label">최대 수신:</span>
                  <span className="attr-value">{queue.attributes.maxReceiveCount}회</span>
                </div>
              </div>
            </div>

            <div className="queue-actions">
              <button className="action-btn primary">📊 통계 보기</button>
              <button className="action-btn">📨 메시지 전송</button>
              <button className="action-btn">🧹 큐 비우기</button>
            </div>
          </div>
        ))}
      </div>

      {queues.length === 0 && !loading && (
        <div className="empty-state">
          <h3>생성된 큐가 없습니다</h3>
          <p>새 큐를 생성하여 메시지 처리를 시작하세요.</p>
          <button 
            className="create-btn"
            onClick={() => setShowCreateForm(true)}
          >
            ➕ 첫 번째 큐 생성하기
          </button>
        </div>
      )}
    </div>
  );
};

export default QueueManagement;
