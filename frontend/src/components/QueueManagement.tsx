import React, { useState, useEffect } from 'react';
import queueService, { 
  Queue, 
  CreateQueueRequest, 
  SendMessageRequest, 
  SendMessageBatchRequest,
  QueueStatistics, 
  MessagePreview,
  MessageResponse
} from '../services/queueService';
import StorageMessagesView from './StorageMessagesView';
import './QueueManagement.css';

const QueueManagement: React.FC = () => {
  const [queues, setQueues] = useState<Queue[]>([]);
  const [queueStats, setQueueStats] = useState<Record<string, QueueStatistics>>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [showMessageForm, setShowMessageForm] = useState<string | null>(null);
  const [showBulkMessageForm, setShowBulkMessageForm] = useState<string | null>(null);
  const [showPeekModal, setShowPeekModal] = useState<string | null>(null);
  const [showReceiveModal, setShowReceiveModal] = useState<string | null>(null);
  const [showStatsModal, setShowStatsModal] = useState<string | null>(null);
  const [storageViewVisible, setStorageViewVisible] = useState<string | null>(null);
  const [peekedMessages, setPeekedMessages] = useState<MessagePreview[]>([]);
  const [receivedMessages, setReceivedMessages] = useState<MessageResponse[]>([]);
  const [newQueue, setNewQueue] = useState({
    queueName: '',
    visibilityTimeoutSeconds: 30,
    messageRetentionPeriod: 1209600,
    delaySeconds: 0,
    maxReceiveCount: 3,
    fifoQueue: false
  });
  const [newMessage, setNewMessage] = useState({
    messageBody: '',
    delaySeconds: 0,
    attributes: {}
  });
  const [bulkMessages, setBulkMessages] = useState([
    { id: '1', messageBody: '', delaySeconds: 0, attributes: {} },
    { id: '2', messageBody: '', delaySeconds: 0, attributes: {} }
  ]);

  useEffect(() => {
    fetchQueues();
  }, []);

  const fetchQueues = async () => {
    try {
      setLoading(true);
      const queuesData = await queueService.getQueues();
      setQueues(queuesData);
      
      // 각 큐의 통계 정보도 가져오기
      const statsPromises = queuesData.map(async (queue) => {
        try {
          const stats = await queueService.getQueueStatistics(queue.queueName);
          return { queueName: queue.queueName, stats };
        } catch (error) {
          console.error(`Failed to fetch stats for queue ${queue.queueName}:`, error);
          // 통계를 가져올 수 없는 경우 기본값 반환
          return { 
            queueName: queue.queueName, 
            stats: {
              queueName: queue.queueName,
              approximateNumberOfMessages: 0,
              approximateNumberOfMessagesNotVisible: 0,
              approximateNumberOfMessagesInDLQ: 0,
              approximateAgeOfOldestMessage: undefined,
              lastUpdated: new Date().toISOString()
            }
          };
        }
      });
      
      const statsResults = await Promise.all(statsPromises);
      const statsMap: Record<string, QueueStatistics> = {};
      statsResults.forEach(result => {
        if (result) {
          statsMap[result.queueName] = result.stats;
        }
      });
      setQueueStats(statsMap);
      
    } catch (error) {
      setError('큐 목록을 가져오는데 실패했습니다: ' + (error as Error).message);
    } finally {
      setLoading(false);
    }
  };

  const handleCreateQueue = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      const request: CreateQueueRequest = {
        queueName: newQueue.queueName,
        attributes: {
          visibilityTimeoutSeconds: newQueue.visibilityTimeoutSeconds,
          messageRetentionPeriod: newQueue.messageRetentionPeriod,
          delaySeconds: newQueue.delaySeconds,
          maxReceiveCount: newQueue.maxReceiveCount,
          fifoQueue: newQueue.fifoQueue
        }
      };
      
      await queueService.createQueue(request);
      setShowCreateForm(false);
      setNewQueue({
        queueName: '',
        visibilityTimeoutSeconds: 30,
        messageRetentionPeriod: 1209600,
        delaySeconds: 0,
        maxReceiveCount: 3,
        fifoQueue: false
      });
      await fetchQueues(); // 목록 새로고침
    } catch (error) {
      setError('큐 생성에 실패했습니다: ' + (error as Error).message);
    }
  };

  const handleDeleteQueue = async (queueName: string) => {
    if (!window.confirm(`정말로 "${queueName}" 큐를 삭제하시겠습니까?`)) {
      return;
    }
    
    try {
      await queueService.deleteQueue(queueName);
      await fetchQueues(); // 목록 새로고침
    } catch (error) {
      setError('큐 삭제에 실패했습니다: ' + (error as Error).message);
    }
  };

  const handleSendMessage = async (queueName: string) => {
    try {
      const request: SendMessageRequest = {
        messageBody: newMessage.messageBody,
        delaySeconds: newMessage.delaySeconds,
        attributes: newMessage.attributes
      };
      
      await queueService.sendMessage(queueName, request);
      setShowMessageForm(null);
      setNewMessage({
        messageBody: '',
        delaySeconds: 0,
        attributes: {}
      });
      await fetchQueues(); // 통계 새로고침
    } catch (error) {
      setError('메시지 전송에 실패했습니다: ' + (error as Error).message);
    }
  };

  const handleSendBulkMessages = async (queueName: string) => {
    try {
      const filteredMessages = bulkMessages.filter(msg => msg.messageBody.trim() !== '');
      if (filteredMessages.length === 0) {
        setError('전송할 메시지가 없습니다.');
        return;
      }

      const request: SendMessageBatchRequest = {
        messages: filteredMessages
      };
      
      const response = await queueService.sendMessageBatch(queueName, request);
      
      if (response.failed.length > 0) {
        const failedCount = response.failed.length;
        const successCount = response.successful.length;
        setError(`일괄 전송 완료: 성공 ${successCount}개, 실패 ${failedCount}개`);
      }
      
      setShowBulkMessageForm(null);
      setBulkMessages([
        { id: '1', messageBody: '', delaySeconds: 0, attributes: {} },
        { id: '2', messageBody: '', delaySeconds: 0, attributes: {} }
      ]);
      await fetchQueues(); // 통계 새로고침
    } catch (error) {
      setError('일괄 메시지 전송에 실패했습니다: ' + (error as Error).message);
    }
  };

  const handlePeekMessages = async (queueName: string) => {
    try {
      const response = await queueService.peekMessages(queueName, 10);
      setPeekedMessages(response.messages);
      setShowPeekModal(queueName);
    } catch (error) {
      setError('메시지 미리보기에 실패했습니다: ' + (error as Error).message);
    }
  };

  const handleReceiveMessages = async (queueName: string) => {
    try {
      const response = await queueService.receiveMessages(queueName, {
        maxNumberOfMessages: 10,
        visibilityTimeoutSeconds: 30
      });
      setReceivedMessages(response.messages);
      setShowReceiveModal(queueName);
      await fetchQueues(); // 통계 새로고침
    } catch (error) {
      setError('메시지 수신에 실패했습니다: ' + (error as Error).message);
    }
  };

  const handleDeleteMessage = async (queueName: string, receiptHandle: string) => {
    try {
      await queueService.deleteMessage(queueName, receiptHandle);
      // 수신된 메시지 목록에서 제거
      setReceivedMessages(prev => prev.filter(msg => msg.receiptHandle !== receiptHandle));
      await fetchQueues(); // 통계 새로고침
    } catch (error) {
      setError('메시지 삭제에 실패했습니다: ' + (error as Error).message);
    }
  };

  const addBulkMessage = () => {
    const newId = (bulkMessages.length + 1).toString();
    setBulkMessages([...bulkMessages, { id: newId, messageBody: '', delaySeconds: 0, attributes: {} }]);
  };

  const removeBulkMessage = (id: string) => {
    if (bulkMessages.length > 1) {
      setBulkMessages(bulkMessages.filter(msg => msg.id !== id));
    }
  };

  const updateBulkMessage = (id: string, field: string, value: any) => {
    setBulkMessages(bulkMessages.map(msg => 
      msg.id === id ? { ...msg, [field]: value } : msg
    ));
  };

  const handlePurgeQueue = async (queueName: string) => {
    if (!window.confirm(`정말로 "${queueName}" 큐의 모든 메시지를 삭제하시겠습니까?`)) {
      return;
    }
    
    try {
      await queueService.purgeQueue(queueName);
      await fetchQueues(); // 통계 새로고침
    } catch (error) {
      setError('큐 비우기에 실패했습니다: ' + (error as Error).message);
    }
  };

  const handleShowStats = (queueName: string) => {
    setShowStatsModal(queueName);
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

      {/* 메시지 전송 폼 */}
      {showMessageForm && (
        <div className="modal-overlay">
          <div className="modal">
            <div className="modal-header">
              <h2>📨 {showMessageForm}에 메시지 전송</h2>
            </div>
            
            <div className="modal-content">
              <form onSubmit={(e) => {
                e.preventDefault();
                handleSendMessage(showMessageForm);
              }}>
                <div className="form-group">
                  <label>메시지 내용:</label>
                  <textarea
                    value={newMessage.messageBody}
                    onChange={(e) => setNewMessage({...newMessage, messageBody: e.target.value})}
                    placeholder="메시지 내용을 입력하세요..."
                    rows={5}
                    required
                  />
                </div>

                <div className="form-group">
                  <label>⏰ 지연 시간 (초):</label>
                  <input
                    type="number"
                    value={newMessage.delaySeconds}
                    onChange={(e) => setNewMessage({...newMessage, delaySeconds: parseInt(e.target.value)})}
                    min="0"
                    max="900"
                    placeholder="0"
                  />
                  <small style={{color: '#6b7280', fontSize: '0.75rem'}}>
                    0~900초 (15분) 범위에서 설정 가능합니다
                  </small>
                </div>

                <div className="form-actions">
                  <button type="submit" className="submit-btn">📤 전송</button>
                  <button 
                    type="button" 
                    className="cancel-btn"
                    onClick={() => setShowMessageForm(null)}
                  >
                    취소
                  </button>
                </div>
              </form>
            </div>
          </div>
        </div>
      )}

      {/* 벌크 메시지 전송 폼 */}
      {showBulkMessageForm && (
        <div className="modal-overlay">
          <div className="modal">
            <div className="modal-header">
              <h2>📦 {showBulkMessageForm}에 벌크 메시지 전송</h2>
            </div>
            
            <div className="modal-content">
              <form onSubmit={(e) => {
                e.preventDefault();
                handleSendBulkMessages(showBulkMessageForm);
              }}>
                <div className="form-group">
                  <label>📝 메시지 목록:</label>
                  {bulkMessages.map((msg, index) => (
                    <div key={msg.id} className="bulk-message-item">
                      <div style={{display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.5rem'}}>
                        <strong style={{color: '#374151', fontSize: '0.875rem'}}>메시지 #{index + 1}</strong>
                        {bulkMessages.length > 2 && (
                          <button 
                            type="button" 
                            className="remove-bulk-message"
                            onClick={() => removeBulkMessage(msg.id)}
                            title="메시지 제거"
                          >
                            ❌
                          </button>
                        )}
                      </div>
                      <textarea
                        value={msg.messageBody}
                        onChange={(e) => updateBulkMessage(msg.id, 'messageBody', e.target.value)}
                        placeholder={`메시지 ${index + 1} 내용을 입력하세요...`}
                        rows={3}
                        required
                      />
                    </div>
                  ))}
                  <button 
                    type="button" 
                    className="add-bulk-message"
                    onClick={addBulkMessage}
                  >
                    ➕ 메시지 추가
                  </button>
                </div>

                <div style={{background: '#f0f9ff', padding: '0.75rem', borderRadius: '0.375rem', marginBottom: '1rem', fontSize: '0.875rem', color: '#0369a1'}}>
                  💡 <strong>팁:</strong> 최대 10개의 메시지를 한 번에 전송할 수 있습니다.
                </div>

                <div className="form-actions">
                  <button type="submit" className="submit-btn">📤 일괄 전송</button>
                  <button 
                    type="button" 
                    className="cancel-btn"
                    onClick={() => setShowBulkMessageForm(null)}
                  >
                    취소
                  </button>
                </div>
              </form>
            </div>
          </div>
        </div>
      )}

      {/* 메시지 미리보기 모달 */}
      {showPeekModal && (
        <div className="modal-overlay">
          <div className="modal">
            <div className="modal-header">
              <h2>{showPeekModal} 메시지 미리보기</h2>
            </div>
            
            <div className="messages-content">
              {peekedMessages.length > 0 ? (
                <div className="messages-list">
                  {peekedMessages.map((message, index) => (
                    <div key={message.messageId} className="message-item">
                      <div className="message-header">
                        <strong>메시지 #{index + 1}</strong>
                        <span className="message-id">{message.messageId}</span>
                      </div>
                      <div className="message-body">
                        <pre>{message.messageBody}</pre>
                      </div>
                      <div className="message-meta">
                        <span>전송 시간: {formatDate(message.sentTimestamp)}</span>
                        <span>수신 횟수: {message.receiveCount}</span>
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                <p>미리볼 메시지가 없습니다.</p>
              )}
            </div>
            
            <div className="form-actions">
              <button 
                type="button" 
                className="cancel-btn"
                onClick={() => setShowPeekModal(null)}
              >
                닫기
              </button>
            </div>
          </div>
        </div>
      )}

      {/* 메시지 수신 모달 */}
      {showReceiveModal && (
        <div className="modal-overlay">
          <div className="modal">
            <div className="modal-header">
              <h2>{showReceiveModal} 메시지 수신</h2>
            </div>
            
            <div className="messages-content">
              {receivedMessages.length > 0 ? (
                <div className="messages-list">
                  {receivedMessages.map((message, index) => (
                    <div key={message.messageId} className="message-item received">
                      <div className="message-header">
                        <strong>수신된 메시지 #{index + 1}</strong>
                        <span className="message-id">{message.messageId}</span>
                      </div>
                      <div className="message-body">
                        <pre>{message.messageBody}</pre>
                      </div>
                      <div className="message-meta">
                        <span>전송 시간: {formatDate(message.sentTimestamp)}</span>
                        <span>Receipt Handle: {message.receiptHandle.substring(0, 8)}...</span>
                      </div>
                      <div className="message-actions">
                        <button 
                          className="delete-message-btn"
                          onClick={() => handleDeleteMessage(showReceiveModal, message.receiptHandle)}
                        >
                          🗑️ 삭제
                        </button>
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                <p>수신된 메시지가 없습니다.</p>
              )}
            </div>
            
            <div className="form-actions">
              <button 
                type="button" 
                className="cancel-btn"
                onClick={() => setShowReceiveModal(null)}
              >
                닫기
              </button>
            </div>
          </div>
        </div>
      )}

      {/* 통계 모달 */}
      {showStatsModal && (
        <div className="modal-overlay">
          <div className="modal">
            <div className="modal-header">
              <h2>{showStatsModal} 통계</h2>
            </div>
            
            <div className="stats-content">
              {queueStats[showStatsModal] ? (
                <div className="stats-grid">
                  <div className="stat-item">
                    <span className="stat-label">대기 중인 메시지:</span>
                    <span className="stat-value">{formatNumber(queueStats[showStatsModal].approximateNumberOfMessages)}</span>
                  </div>
                  <div className="stat-item">
                    <span className="stat-label">처리 중인 메시지:</span>
                    <span className="stat-value">{formatNumber(queueStats[showStatsModal].approximateNumberOfMessagesNotVisible)}</span>
                  </div>
                  <div className="stat-item">
                    <span className="stat-label">DLQ 메시지:</span>
                    <span className="stat-value">{formatNumber(queueStats[showStatsModal].approximateNumberOfMessagesInDLQ)}</span>
                  </div>
                  <div className="stat-item">
                    <span className="stat-label">가장 오래된 메시지:</span>
                    <span className="stat-value">
                      {queueStats[showStatsModal].approximateAgeOfOldestMessage 
                        ? `${queueStats[showStatsModal].approximateAgeOfOldestMessage}초` 
                        : 'N/A'}
                    </span>
                  </div>
                  <div className="stat-item">
                    <span className="stat-label">마지막 업데이트:</span>
                    <span className="stat-value">{formatDate(queueStats[showStatsModal].lastUpdated)}</span>
                  </div>
                </div>
              ) : (
                <p>통계 정보를 불러올 수 없습니다.</p>
              )}
            </div>
            
            <div className="form-actions">
              <button 
                className="cancel-btn"
                onClick={() => setShowStatsModal(null)}
              >
                닫기
              </button>
            </div>
          </div>
        </div>
      )}

      <div className="queue-grid">
        {queues.map((queue) => {
          const stats = queueStats[queue.queueName];
          return (
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
                  <span className="value">{stats ? formatNumber(stats.approximateNumberOfMessages) : 'N/A'}</span>
                </div>
                <div className="info-item">
                  <span className="label">처리 중:</span>
                  <span className="value">{stats ? formatNumber(stats.approximateNumberOfMessagesNotVisible) : 'N/A'}</span>
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
              <button 
                className="action-btn primary"
                onClick={() => handleShowStats(queue.queueName)}
              >
                📊 통계 보기
              </button>
              <button 
                className="action-btn"
                onClick={() => setShowMessageForm(queue.queueName)}
              >
                📨 메시지 전송
              </button>
              <button 
                className="action-btn"
                onClick={() => setShowBulkMessageForm(queue.queueName)}
              >
                📦 일괄 전송
              </button>
              <button 
                className="action-btn"
                onClick={() => handlePurgeQueue(queue.queueName)}
              >
                🧹 큐 비우기
              </button>
              <button 
                className="action-btn"
                onClick={() => handlePeekMessages(queue.queueName)}
              >
                👀 메시지 미리보기
              </button>
              <button 
                className="action-btn"
                onClick={() => handleReceiveMessages(queue.queueName)}
              >
                📥 메시지 수신
              </button>
              <button 
                className="action-btn storage"
                onClick={() => setStorageViewVisible(queue.queueName)}
                title="계층별 메시지 보기"
              >
                🗂️ 스토리지 계층
              </button>
            </div>
          </div>
        );
        })}
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

      {/* 스토리지 계층별 메시지 보기 모달 */}
      {storageViewVisible && (
        <StorageMessagesView
          queueName={storageViewVisible}
          visible={true}
          onClose={() => setStorageViewVisible(null)}
        />
      )}
    </div>
  );
};

export default QueueManagement;
