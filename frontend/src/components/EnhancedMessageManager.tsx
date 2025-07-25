import React, { useState, useEffect, useCallback } from 'react';
import { MessageResponse, MessagePreview } from '../types/sqs';
import queueService from '../services/queueService';
import AdvancedMessageSearch from './AdvancedMessageSearch';
import IntelligentBatchProcessor from './IntelligentBatchProcessor';
import StorageTierManager from './StorageTierManager';
import './EnhancedMessageManager.css';

interface EnhancedMessageManagerProps {
  queueName: string;
}

type TabType = 'overview' | 'send' | 'search' | 'batch' | 'storage';

const EnhancedMessageManager: React.FC<EnhancedMessageManagerProps> = ({ queueName }) => {
  const [activeTab, setActiveTab] = useState<TabType>('overview');
  const [messages, setMessages] = useState<MessageResponse[]>([]);
  const [previews, setPreviews] = useState<MessagePreview[]>([]);
  const [loading, setLoading] = useState(false);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [message, setMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null);
  const [selectedMessages, setSelectedMessages] = useState<string[]>([]);
  const [autoRefresh, setAutoRefresh] = useState(false);

  // Single message sending state
  const [singleMessage, setSingleMessage] = useState({
    body: '',
    delaySeconds: 0,
    attributes: '{}',
    messageGroupId: '',
    messageDeduplicationId: '',
  });

  // Queue statistics
  const [queueStats, setQueueStats] = useState<any>(null);

  const receiveMessages = async () => {
    setLoading(true);
    try {
      const response = await queueService.receiveMessages(queueName, { maxNumberOfMessages: 10 });
      setMessages(response.messages);
      setMessage({ type: 'success', text: `${response.messages.length}개의 메시지를 수신했습니다` });
    } catch (error) {
      setMessage({ type: 'error', text: '메시지 수신 중 오류가 발생했습니다' });
    } finally {
      setLoading(false);
    }
  };

  const peekMessages = useCallback(async () => {
    setPreviewLoading(true);
    try {
      const response = await queueService.peekMessages(queueName, 20);
      setPreviews(response.messages);
      setMessage({ type: 'success', text: `${response.messages.length}개의 메시지를 미리보기했습니다` });
    } catch (error) {
      setMessage({ type: 'error', text: '메시지 미리보기 중 오류가 발생했습니다' });
    } finally {
      setPreviewLoading(false);
    }
  }, [queueName]);

  const sendSingleMessage = async () => {
    if (!singleMessage.body.trim()) {
      setMessage({ type: 'error', text: '메시지 내용을 입력해주세요' });
      return;
    }

    try {
      let attributes = {};
      try {
        attributes = JSON.parse(singleMessage.attributes);
      } catch {
        // Ignore invalid JSON
      }

      const requestBody: any = {
        messageBody: singleMessage.body,
        delaySeconds: singleMessage.delaySeconds,
        attributes,
      };

      // Add FIFO-specific fields if provided
      if (singleMessage.messageGroupId.trim()) {
        requestBody.messageGroupId = singleMessage.messageGroupId.trim();
      }
      if (singleMessage.messageDeduplicationId.trim()) {
        requestBody.messageDeduplicationId = singleMessage.messageDeduplicationId.trim();
      }

      await queueService.sendMessage(queueName, requestBody);

      setMessage({ type: 'success', text: '메시지가 성공적으로 전송되었습니다' });
      setSingleMessage({ 
        body: '', 
        delaySeconds: 0, 
        attributes: '{}',
        messageGroupId: '',
        messageDeduplicationId: ''
      });
      
      // Auto refresh if enabled
      if (autoRefresh) {
        setTimeout(() => {
          peekMessages();
          loadQueueStats();
        }, 1000);
      }
    } catch (error) {
      setMessage({ type: 'error', text: '메시지 전송 중 오류가 발생했습니다' });
    }
  };

  const deleteMessage = async (receiptHandle: string) => {
    try {
      await queueService.deleteMessage(queueName, receiptHandle);
      setMessage({ type: 'success', text: '메시지가 삭제되었습니다' });
      setMessages(messages.filter((msg) => msg.receiptHandle !== receiptHandle));
      
      if (autoRefresh) {
        loadQueueStats();
      }
    } catch (error) {
      setMessage({ type: 'error', text: '메시지 삭제 중 오류가 발생했습니다' });
    }
  };

  const deleteSelectedMessages = async () => {
    if (selectedMessages.length === 0) {
      setMessage({ type: 'error', text: '삭제할 메시지를 선택해주세요' });
      return;
    }

    try {
      // Note: queueService doesn't have batch delete, so delete one by one
      let successful = 0;
      let failed = 0;
      
      for (const receiptHandle of selectedMessages) {
        try {
          await queueService.deleteMessage(queueName, receiptHandle);
          successful++;
        } catch {
          failed++;
        }
      }
      setMessage({
        type: failed === 0 ? 'success' : 'error',
        text: `일괄 삭제 완료: 성공 ${successful}개, 실패 ${failed}개`,
      });
      setMessages(messages.filter((msg) => !selectedMessages.includes(msg.receiptHandle)));
      setSelectedMessages([]);
      
      if (autoRefresh) {
        loadQueueStats();
      }
    } catch (error) {
      setMessage({ type: 'error', text: '일괄 삭제 중 오류가 발생했습니다' });
    }
  };

  const purgeQueue = async () => {
    if (!window.confirm('큐의 모든 메시지를 삭제하시겠습니까?')) {
      return;
    }

    try {
      await queueService.purgeQueue(queueName);
      setMessage({ type: 'success', text: '큐의 모든 메시지가 삭제되었습니다' });
      setMessages([]);
      setPreviews([]);
      
      if (autoRefresh) {
        setTimeout(loadQueueStats, 2000); // Give some time for purge to complete
      }
    } catch (error) {
      setMessage({ type: 'error', text: '큐 퍼지 중 오류가 발생했습니다' });
    }
  };

  const loadQueueStats = useCallback(async () => {
    try {
      const response = await queueService.getQueueStatistics(queueName);
      setQueueStats(response);
    } catch (error) {
      // Silently fail for stats
    }
  }, [queueName]);

  // Auto refresh effect
  useEffect(() => {
    if (autoRefresh && activeTab === 'overview') {
      const interval = setInterval(() => {
        peekMessages();
        loadQueueStats();
      }, 5000);
      return () => clearInterval(interval);
    }
  }, [autoRefresh, activeTab, queueName, peekMessages, loadQueueStats]);

  // Initial load
  useEffect(() => {
    if (activeTab === 'overview') {
      peekMessages();
      loadQueueStats();
    }
  }, [queueName, activeTab, peekMessages, loadQueueStats]);

  // Message cleanup effect
  useEffect(() => {
    if (message) {
      const timer = setTimeout(() => setMessage(null), 5000);
      return () => clearTimeout(timer);
    }
  }, [message]);

  const renderTabContent = () => {
    switch (activeTab) {
      case 'overview':
        return (
          <div className="tab-content">
            {/* Queue Statistics */}
            {queueStats && (
              <div className="queue-stats">
                <h4>Queue Statistics</h4>
                <div className="stats-grid">
                  <div className="stat-card">
                    <div className="stat-value">{queueStats.approximateNumberOfMessages}</div>
                    <div className="stat-label">Available Messages</div>
                  </div>
                  <div className="stat-card">
                    <div className="stat-value">{queueStats.approximateNumberOfMessagesNotVisible}</div>
                    <div className="stat-label">In Flight Messages</div>
                  </div>
                  <div className="stat-card">
                    <div className="stat-value">{queueStats.approximateNumberOfMessagesInDLQ}</div>
                    <div className="stat-label">DLQ Messages</div>
                  </div>
                  <div className="stat-card">
                    <div className="stat-value">
                      {queueStats.approximateAgeOfOldestMessage 
                        ? `${Math.floor(queueStats.approximateAgeOfOldestMessage / 1000)}s`
                        : 'N/A'
                      }
                    </div>
                    <div className="stat-label">Oldest Message Age</div>
                  </div>
                </div>
              </div>
            )}

            {/* Quick Actions */}
            <div className="quick-actions">
              <div className="section-header">
                <h4>Quick Actions</h4>
                <div className="action-controls">
                  <label className="auto-refresh-label">
                    <input
                      type="checkbox"
                      checked={autoRefresh}
                      onChange={(e) => setAutoRefresh(e.target.checked)}
                    />
                    Auto Refresh
                  </label>
                  <button onClick={peekMessages} disabled={previewLoading} className="btn btn-info btn-sm">
                    {previewLoading ? 'Loading...' : 'Refresh Preview'}
                  </button>
                  <button onClick={receiveMessages} disabled={loading} className="btn btn-success btn-sm">
                    {loading ? 'Receiving...' : 'Receive Messages'}
                  </button>
                </div>
              </div>
            </div>

            {/* Single Message Sending */}
            <div className="single-message-form">
              <h4>Send Single Message</h4>
              <div className="form-grid">
                <div className="form-group span-2">
                  <label>Message Body</label>
                  <textarea
                    value={singleMessage.body}
                    onChange={(e) => setSingleMessage({ ...singleMessage, body: e.target.value })}
                    placeholder="Enter your message content..."
                    className="form-control"
                    rows={4}
                  />
                </div>

                <div className="form-group">
                  <label>Delay (seconds)</label>
                  <input
                    type="number"
                    value={singleMessage.delaySeconds}
                    onChange={(e) => setSingleMessage({ ...singleMessage, delaySeconds: parseInt(e.target.value) || 0 })}
                    className="form-control"
                    min="0"
                    max="900"
                  />
                </div>

                <div className="form-group">
                  <label>Message Group ID (FIFO)</label>
                  <input
                    type="text"
                    value={singleMessage.messageGroupId}
                    onChange={(e) => setSingleMessage({ ...singleMessage, messageGroupId: e.target.value })}
                    placeholder="Optional for FIFO queues"
                    className="form-control"
                  />
                </div>

                <div className="form-group">
                  <label>Deduplication ID (FIFO)</label>
                  <input
                    type="text"
                    value={singleMessage.messageDeduplicationId}
                    onChange={(e) => setSingleMessage({ ...singleMessage, messageDeduplicationId: e.target.value })}
                    placeholder="Optional for FIFO queues"
                    className="form-control"
                  />
                </div>

                <div className="form-group span-2">
                  <label>Attributes (JSON)</label>
                  <input
                    type="text"
                    value={singleMessage.attributes}
                    onChange={(e) => setSingleMessage({ ...singleMessage, attributes: e.target.value })}
                    placeholder='{"priority": "high", "category": "order"}'
                    className="form-control"
                  />
                </div>
              </div>

              <div className="form-actions">
                <button onClick={sendSingleMessage} className="btn btn-primary">
                  Send Message
                </button>
              </div>
            </div>

            {/* Received Messages */}
            <div className="received-messages">
              <div className="section-header">
                <h4>Received Messages ({messages.length})</h4>
                <div className="message-actions">
                  {selectedMessages.length > 0 && (
                    <button onClick={deleteSelectedMessages} className="btn btn-warning btn-sm">
                      Delete Selected ({selectedMessages.length})
                    </button>
                  )}
                  <button onClick={purgeQueue} className="btn btn-danger btn-sm">
                    Purge Queue
                  </button>
                </div>
              </div>

              {messages.length === 0 ? (
                <div className="empty-state">
                  No messages received. Click "Receive Messages" to get messages from the queue.
                </div>
              ) : (
                <div className="messages-list">
                  {messages.map((msg, index) => (
                    <div key={msg.receiptHandle} className="message-item">
                      <div className="message-header">
                        <div className="message-info">
                          <label className="checkbox-label">
                            <input
                              type="checkbox"
                              checked={selectedMessages.includes(msg.receiptHandle)}
                              onChange={(e) => {
                                if (e.target.checked) {
                                  setSelectedMessages([...selectedMessages, msg.receiptHandle]);
                                } else {
                                  setSelectedMessages(selectedMessages.filter(h => h !== msg.receiptHandle));
                                }
                              }}
                            />
                            <span className="message-id">ID: {msg.messageId}</span>
                          </label>
                        </div>
                        <button
                          onClick={() => deleteMessage(msg.receiptHandle)}
                          className="btn btn-danger btn-sm"
                        >
                          Delete
                        </button>
                      </div>

                      <div className="message-body">{msg.messageBody}</div>

                      {Object.keys(msg.attributes).length > 0 && (
                        <div className="message-attributes">
                          <strong>Attributes:</strong>
                          <pre>{JSON.stringify(msg.attributes, null, 2)}</pre>
                        </div>
                      )}

                      <div className="message-meta">
                        <span>Sent: {new Date(msg.sentTimestamp).toLocaleString()}</span>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>

            {/* Message Preview */}
            <div className="message-preview">
              <div className="section-header">
                <h4>Message Preview ({previews.length})</h4>
                <span className="preview-note">Messages remain in queue</span>
              </div>

              {previews.length === 0 ? (
                <div className="empty-state">
                  No messages in queue or preview not loaded.
                </div>
              ) : (
                <div className="preview-list">
                  {previews.slice(0, 10).map((msg, index) => (
                    <div key={msg.messageId} className="preview-item">
                      <div className="preview-header">
                        <span className="message-id">ID: {msg.messageId}</span>
                        <div className="preview-meta">
                          <span>Receive Count: {msg.receiveCount}</span>
                          <span>Sent: {new Date(msg.sentTimestamp).toLocaleString()}</span>
                        </div>
                      </div>
                      
                      <div className="preview-body">
                        {msg.messageBody.length > 200 
                          ? `${msg.messageBody.substring(0, 200)}...`
                          : msg.messageBody
                        }
                      </div>

                      {Object.keys(msg.attributes).length > 0 && (
                        <div className="preview-attributes">
                          <strong>Attributes:</strong>
                          <span>{JSON.stringify(msg.attributes)}</span>
                        </div>
                      )}
                    </div>
                  ))}
                  
                  {previews.length > 10 && (
                    <div className="more-messages">
                      ... and {previews.length - 10} more messages
                    </div>
                  )}
                </div>
              )}
            </div>
          </div>
        );

      case 'search':
        return <AdvancedMessageSearch queueName={queueName} />;

      case 'batch':
        return <IntelligentBatchProcessor queueName={queueName} />;

      case 'storage':
        return <StorageTierManager queueName={queueName} />;

      default:
        return <div>Tab content not implemented</div>;
    }
  };

  return (
    <div className="enhanced-message-manager">
      {message && (
        <div className={`global-message ${message.type}`}>
          {message.text}
        </div>
      )}

      {/* Tab Navigation */}
      <div className="tab-navigation">
        <button
          className={`tab-button ${activeTab === 'overview' ? 'active' : ''}`}
          onClick={() => setActiveTab('overview')}
        >
          <span className="tab-icon">📊</span>
          Overview
        </button>
        <button
          className={`tab-button ${activeTab === 'search' ? 'active' : ''}`}
          onClick={() => setActiveTab('search')}
        >
          <span className="tab-icon">🔍</span>
          Advanced Search
        </button>
        <button
          className={`tab-button ${activeTab === 'batch' ? 'active' : ''}`}
          onClick={() => setActiveTab('batch')}
        >
          <span className="tab-icon">⚡</span>
          Intelligent Batch
        </button>
        <button
          className={`tab-button ${activeTab === 'storage' ? 'active' : ''}`}
          onClick={() => setActiveTab('storage')}
        >
          <span className="tab-icon">🗄️</span>
          Storage Tiers
        </button>
      </div>

      {/* Tab Content */}
      <div className="tab-container">
        {renderTabContent()}
      </div>
    </div>
  );
};

export default EnhancedMessageManager;