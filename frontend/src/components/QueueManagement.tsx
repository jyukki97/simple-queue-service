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
    fifoQueue: false,
    contentBasedDeduplication: false
  });
  const [newMessage, setNewMessage] = useState({
    messageBody: '',
    delaySeconds: 0,
    attributes: {},
    messageGroupId: '',
    messageDeduplicationId: ''
  });
  const [bulkMessages, setBulkMessages] = useState([
    { id: '1', messageBody: '', delaySeconds: 0, attributes: {}, messageGroupId: '', messageDeduplicationId: '' },
    { id: '2', messageBody: '', delaySeconds: 0, attributes: {}, messageGroupId: '', messageDeduplicationId: '' }
  ]);
  const [receiveRequest, setReceiveRequest] = useState({
    maxNumberOfMessages: 1,
    visibilityTimeoutSeconds: 30,
    waitTimeSeconds: 0,
    messageGroupId: ''
  });

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
          fifoQueue: newQueue.fifoQueue,
          contentBasedDeduplication: newQueue.contentBasedDeduplication
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
        fifoQueue: false,
        contentBasedDeduplication: false
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
      const queue = queues.find(q => q.queueName === queueName);
      
      const request: SendMessageRequest = {
        messageBody: newMessage.messageBody,
        delaySeconds: newMessage.delaySeconds,
        attributes: newMessage.attributes,
        messageGroupId: newMessage.messageGroupId || undefined,
        messageDeduplicationId: newMessage.messageDeduplicationId || undefined
      };
      
      // FIFO 큐이고 ContentBasedDeduplication이 false이면 MessageDeduplicationId 필수
      if (queue?.attributes.fifoQueue && !queue.attributes.contentBasedDeduplication) {
        if (!request.messageDeduplicationId || request.messageDeduplicationId.trim() === '') {
          // 자동으로 UUID 생성
          request.messageDeduplicationId = `auto-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
        }
      }
      
      await queueService.sendMessage(queueName, request);
      setShowMessageForm(null);
      setNewMessage({
        messageBody: '',
        delaySeconds: 0,
        attributes: {},
        messageGroupId: '',
        messageDeduplicationId: ''
      });
      await fetchQueues(); // 통계 새로고침
    } catch (error) {
      setError('메시지 전송에 실패했습니다: ' + (error as Error).message);
    }
  };

  const handleSendBulkMessages = async (queueName: string) => {
    try {
      const queue = queues.find(q => q.queueName === queueName);
      const filteredMessages = bulkMessages.filter(msg => msg.messageBody.trim() !== '');
      
      if (filteredMessages.length === 0) {
        setError('전송할 메시지가 없습니다.');
        return;
      }

      // FIFO 큐이고 ContentBasedDeduplication이 false이면 MessageDeduplicationId 필수이므로 자동 생성
      const processedMessages = filteredMessages.map(msg => {
        const processedMsg = { ...msg };
        
        if (queue?.attributes.fifoQueue && !queue.attributes.contentBasedDeduplication) {
          if (!processedMsg.messageDeduplicationId || processedMsg.messageDeduplicationId.trim() === '') {
            // 자동으로 UUID 생성
            processedMsg.messageDeduplicationId = `auto-${Date.now()}-${Math.random().toString(36).substr(2, 9)}-${msg.id}`;
          }
        }
        
        return processedMsg;
      });

      const request: SendMessageBatchRequest = {
        messages: processedMessages
      };
      
      const response = await queueService.sendMessageBatch(queueName, request);
      
      if (response.failed.length > 0) {
        const failedCount = response.failed.length;
        const successCount = response.successful.length;
        setError(`일괄 전송 완료: 성공 ${successCount}개, 실패 ${failedCount}개`);
      }
      
      setShowBulkMessageForm(null);
      setBulkMessages([
        { id: '1', messageBody: '', delaySeconds: 0, attributes: {}, messageGroupId: '', messageDeduplicationId: '' },
        { id: '2', messageBody: '', delaySeconds: 0, attributes: {}, messageGroupId: '', messageDeduplicationId: '' }
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
      const queue = queues.find(q => q.queueName === queueName);
      const requestParams: any = {
        maxNumberOfMessages: receiveRequest.maxNumberOfMessages,
        visibilityTimeoutSeconds: receiveRequest.visibilityTimeoutSeconds,
        waitTimeSeconds: receiveRequest.waitTimeSeconds
      };
      
      // FIFO 큐인 경우 messageGroupId 추가
      if (queue?.attributes.fifoQueue && receiveRequest.messageGroupId) {
        requestParams.messageGroupId = receiveRequest.messageGroupId;
      }
      
      const response = await queueService.receiveMessages(queueName, requestParams);
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
    setBulkMessages([...bulkMessages, { id: newId, messageBody: '', delaySeconds: 0, attributes: {}, messageGroupId: '', messageDeduplicationId: '' }]);
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

  const formatDate = (dateValue: string | number) => {
    const date = typeof dateValue === 'number' ? new Date(dateValue) : new Date(dateValue);
    return date.toLocaleDateString('ko-KR', {
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

              {/* FIFO 큐 전용 옵션들 */}
              {newQueue.fifoQueue && (
                <div className="form-group checkbox-group">
                  <label>
                    <input
                      type="checkbox"
                      checked={newQueue.contentBasedDeduplication}
                      onChange={(e) => setNewQueue({...newQueue, contentBasedDeduplication: e.target.checked})}
                    />
                    Content-Based Deduplication
                  </label>
                  <small style={{color: '#6b7280', fontSize: '0.75rem', display: 'block', marginTop: '0.25rem'}}>
                    활성화하면 메시지 내용을 기반으로 자동 중복 제거. 비활성화하면 MessageDeduplicationId가 필수입니다.
                  </small>
                </div>
              )}

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

                {/* FIFO 큐용 필드들 */}
                {queues.find(q => q.queueName === showMessageForm)?.attributes.fifoQueue && (
                  <>
                    <div className="form-group">
                      <label>🏷️ Message Group ID:</label>
                      <input
                        type="text"
                        value={newMessage.messageGroupId}
                        onChange={(e) => setNewMessage({...newMessage, messageGroupId: e.target.value})}
                        placeholder="메시지 그룹 ID를 입력하세요"
                        required
                      />
                      <small style={{color: '#6b7280', fontSize: '0.75rem'}}>
                        FIFO 큐에서는 필수입니다. 같은 그룹의 메시지는 순서가 보장됩니다.
                      </small>
                    </div>
                    
                    <div className="form-group">
                      <label>🔑 Message Deduplication ID (선택사항):</label>
                      <input
                        type="text"
                        value={newMessage.messageDeduplicationId}
                        onChange={(e) => setNewMessage({...newMessage, messageDeduplicationId: e.target.value})}
                        placeholder="중복 제거 ID를 입력하세요"
                      />
                      <small style={{color: '#6b7280', fontSize: '0.75rem'}}>
                        중복 메시지 방지용. 비워두면 자동으로 고유 ID가 생성됩니다.
                      </small>
                    </div>
                  </>
                )}

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
                      
                      {/* FIFO 큐용 필드들 */}
                      {queues.find(q => q.queueName === showBulkMessageForm)?.attributes.fifoQueue && (
                        <div style={{marginTop: '0.5rem'}}>
                          <div style={{display: 'flex', gap: '0.5rem'}}>
                            <div style={{flex: 1}}>
                              <label style={{display: 'block', fontSize: '0.75rem', color: '#6b7280', marginBottom: '0.25rem'}}>
                                Message Group ID:
                              </label>
                              <input
                                type="text"
                                value={msg.messageGroupId || ''}
                                onChange={(e) => updateBulkMessage(msg.id, 'messageGroupId', e.target.value)}
                                placeholder="그룹 ID"
                                style={{width: '100%', fontSize: '0.75rem'}}
                                required
                              />
                            </div>
                            <div style={{flex: 1}}>
                              <label style={{display: 'block', fontSize: '0.75rem', color: '#6b7280', marginBottom: '0.25rem'}}>
                                Deduplication ID:
                              </label>
                              <input
                                type="text"
                                value={msg.messageDeduplicationId || ''}
                                onChange={(e) => updateBulkMessage(msg.id, 'messageDeduplicationId', e.target.value)}
                                placeholder="중복 제거 ID (비워두면 자동 생성)"
                                style={{width: '100%', fontSize: '0.75rem'}}
                              />
                            </div>
                          </div>
                        </div>
                      )}
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
                        {message.messageGroupId && (
                          <span>Group ID: {message.messageGroupId}</span>
                        )}
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
              <h2>{showReceiveModal.includes('-settings') ? 
                `${showReceiveModal.replace('-settings', '')} 메시지 수신 설정` : 
                `${showReceiveModal} 메시지 수신`}
              </h2>
            </div>
            
            {showReceiveModal.includes('-settings') ? (
              // FIFO 큐 수신 설정 화면
              <div className="modal-content">
                <form onSubmit={(e) => {
                  e.preventDefault();
                  const queueName = showReceiveModal.replace('-settings', '');
                  handleReceiveMessages(queueName);
                }}>
                  <div className="form-group">
                    <label>🔢 최대 메시지 수:</label>
                    <input
                      type="number"
                      value={receiveRequest.maxNumberOfMessages}
                      onChange={(e) => setReceiveRequest({...receiveRequest, maxNumberOfMessages: parseInt(e.target.value)})}
                      min="1"
                      max="10"
                    />
                  </div>
                  
                  <div className="form-group">
                    <label>⏰ 가시성 타임아웃 (초):</label>
                    <input
                      type="number"
                      value={receiveRequest.visibilityTimeoutSeconds}
                      onChange={(e) => setReceiveRequest({...receiveRequest, visibilityTimeoutSeconds: parseInt(e.target.value)})}
                      min="0"
                      max="43200"
                    />
                  </div>
                  
                  <div className="form-group">
                    <label>⌛ 대기 시간 (초):</label>
                    <input
                      type="number"
                      value={receiveRequest.waitTimeSeconds}
                      onChange={(e) => setReceiveRequest({...receiveRequest, waitTimeSeconds: parseInt(e.target.value)})}
                      min="0"
                      max="20"
                    />
                  </div>
                  
                  <div className="form-group">
                    <label>🏷️ Message Group ID (FIFO 필수):</label>
                    <input
                      type="text"
                      value={receiveRequest.messageGroupId}
                      onChange={(e) => setReceiveRequest({...receiveRequest, messageGroupId: e.target.value})}
                      placeholder="메시지 그룹 ID를 입력하세요"
                      required
                    />
                    <small style={{color: '#6b7280', fontSize: '0.75rem'}}>
                      해당 그룹의 메시지만 수신됩니다.
                    </small>
                  </div>
                  
                  <div className="form-actions">
                    <button type="submit" className="submit-btn">📥 수신 시작</button>
                    <button 
                      type="button" 
                      className="cancel-btn"
                      onClick={() => setShowReceiveModal(null)}
                    >
                      취소
                    </button>
                  </div>
                </form>
              </div>
            ) : (
              // 수신된 메시지 표시 화면
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
                          {message.messageGroupId && (
                            <span>Group ID: {message.messageGroupId}</span>
                          )}
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
            )}
            
            {!showReceiveModal.includes('-settings') && (
              <div className="form-actions">
                <button 
                  type="button" 
                  className="cancel-btn"
                  onClick={() => setShowReceiveModal(null)}
                >
                  닫기
                </button>
              </div>
            )}
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
                onClick={() => {
                  const currentQueue = queues.find(q => q.queueName === queue.queueName);
                  if (currentQueue?.attributes.fifoQueue) {
                    // FIFO 큐인 경우 수신 설정 모달 먼저 표시
                    setShowReceiveModal(`${queue.queueName}-settings`);
                  } else {
                    // 일반 큐인 경우 바로 수신
                    handleReceiveMessages(queue.queueName);
                  }
                }}
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
