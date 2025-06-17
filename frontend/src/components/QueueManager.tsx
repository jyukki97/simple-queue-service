import React, { useState, useEffect } from 'react';
import { Queue } from '../types/sqs';
import { sqsApi } from '../services/sqsApi';
import './QueueManager.css';

interface QueueManagerProps {
  onQueueSelect: (queueName: string) => void;
  selectedQueue: string | null;
}

const QueueManager: React.FC<QueueManagerProps> = ({ onQueueSelect, selectedQueue }) => {
  const [queues, setQueues] = useState<Queue[]>([]);
  const [loading, setLoading] = useState(false);
  const [newQueueName, setNewQueueName] = useState('');
  const [message, setMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null);

  const loadQueues = async () => {
    setLoading(true);
    try {
      const response = await sqsApi.listQueues();
      if (response.success && response.data) {
        setQueues(response.data);
      } else {
        setMessage({ type: 'error', text: response.error?.message || '큐 목록 조회 실패' });
      }
    } catch (error) {
      setMessage({ type: 'error', text: '네트워크 오류가 발생했습니다' });
    } finally {
      setLoading(false);
    }
  };

  const createQueue = async () => {
    if (!newQueueName.trim()) {
      setMessage({ type: 'error', text: '큐 이름을 입력해주세요' });
      return;
    }

    try {
      const response = await sqsApi.createQueue({
        queueName: newQueueName,
        attributes: {
          visibilityTimeoutSeconds: 30,
          messageRetentionPeriod: 1209600,
          delaySeconds: 0,
          maxReceiveCount: 3,
        },
      });

      if (response.success) {
        setMessage({ type: 'success', text: `큐 '${newQueueName}' 생성 완료` });
        setNewQueueName('');
        loadQueues();
      } else {
        setMessage({ type: 'error', text: response.error?.message || '큐 생성 실패' });
      }
    } catch (error) {
      setMessage({ type: 'error', text: '큐 생성 중 오류가 발생했습니다' });
    }
  };

  const deleteQueue = async (queueName: string) => {
    if (!window.confirm(`큐 '${queueName}'을(를) 삭제하시겠습니까?`)) {
      return;
    }

    try {
      const response = await sqsApi.deleteQueue(queueName);
      if (response.success) {
        setMessage({ type: 'success', text: `큐 '${queueName}' 삭제 완료` });
        if (selectedQueue === queueName) {
          onQueueSelect('');
        }
        loadQueues();
      } else {
        setMessage({ type: 'error', text: response.error?.message || '큐 삭제 실패' });
      }
    } catch (error) {
      setMessage({ type: 'error', text: '큐 삭제 중 오류가 발생했습니다' });
    }
  };

  useEffect(() => {
    loadQueues();
  }, []);

  useEffect(() => {
    if (message) {
      const timer = setTimeout(() => setMessage(null), 3000);
      return () => clearTimeout(timer);
    }
  }, [message]);

  return (
    <div className="queue-manager">
      {message && (
        <div className={`message ${message.type}`}>
          {message.text}
        </div>
      )}

      {/* 큐 생성 */}
      <div className="create-queue-form">
        <h3>새 큐 생성</h3>
        <div className="form-row">
          <input
            type="text"
            value={newQueueName}
            onChange={(e) => setNewQueueName(e.target.value)}
            placeholder="큐 이름 입력"
            onKeyPress={(e) => e.key === 'Enter' && createQueue()}
          />
          <button onClick={createQueue} className="btn btn-primary">
            생성
          </button>
        </div>
      </div>

      {/* 큐 목록 */}
      <div className="queue-list">
        <div className="form-row">
          <h3>큐 목록</h3>
          <button
            onClick={loadQueues}
            disabled={loading}
            className="btn btn-secondary"
          >
            {loading ? '로딩...' : '새로고침'}
          </button>
        </div>

        {loading ? (
          <div className="loading">큐 목록을 불러오는 중...</div>
        ) : queues.length === 0 ? (
          <div className="empty-state">생성된 큐가 없습니다.</div>
        ) : (
          <div className="queue-items">
            {queues.map((queue) => (
              <div
                key={queue.queueName}
                className={`queue-item ${selectedQueue === queue.queueName ? 'selected' : ''}`}
                onClick={() => onQueueSelect(queue.queueName)}
              >
                <div>
                  <div className="queue-name">{queue.queueName}</div>
                  <div style={{ fontSize: '0.85rem', color: '#6c757d' }}>
                    가시성 타임아웃: {queue.attributes.visibilityTimeoutSeconds}초 |
                    최대 수신 횟수: {queue.attributes.maxReceiveCount}
                  </div>
                </div>
                <div className="queue-actions">
                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      deleteQueue(queue.queueName);
                    }}
                    className="btn btn-danger btn-sm"
                  >
                    삭제
                  </button>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
};

export default QueueManager;
