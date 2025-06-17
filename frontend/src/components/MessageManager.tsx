import React, { useState, useEffect } from 'react';
import { MessageResponse, BatchMessageRequest } from '../types/sqs';
import { sqsApi } from '../services/sqsApi';
import './MessageManager.css';

interface MessageManagerProps {
  queueName: string;
}

const MessageManager: React.FC<MessageManagerProps> = ({ queueName }) => {
  const [messages, setMessages] = useState<MessageResponse[]>([]);
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null);
  const [selectedMessages, setSelectedMessages] = useState<string[]>([]);

  // 단일 메시지 전송
  const [singleMessage, setSingleMessage] = useState({
    body: '',
    delaySeconds: 0,
    attributes: '{}',
  });

  // 일괄 메시지 전송
  const [batchMessages, setBatchMessages] = useState<BatchMessageRequest[]>([
    { id: 'msg1', messageBody: '', delaySeconds: 0 },
  ]);

  const receiveMessages = async () => {
    setLoading(true);
    try {
      const response = await sqsApi.receiveMessages(queueName, 10);
      if (response.success && response.data) {
        setMessages(response.data);
        setMessage({ type: 'success', text: `${response.data.length}개의 메시지를 수신했습니다` });
      } else {
        setMessage({ type: 'error', text: response.error?.message || '메시지 수신 실패' });
      }
    } catch (error) {
      setMessage({ type: 'error', text: '메시지 수신 중 오류가 발생했습니다' });
    } finally {
      setLoading(false);
    }
  };

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
        // 잘못된 JSON이면 빈 객체로 설정
      }

      const response = await sqsApi.sendMessage(queueName, {
        messageBody: singleMessage.body,
        delaySeconds: singleMessage.delaySeconds,
        attributes,
      });

      if (response.success) {
        setMessage({ type: 'success', text: '메시지가 성공적으로 전송되었습니다' });
        setSingleMessage({ body: '', delaySeconds: 0, attributes: '{}' });
      } else {
        setMessage({ type: 'error', text: response.error?.message || '메시지 전송 실패' });
      }
    } catch (error) {
      setMessage({ type: 'error', text: '메시지 전송 중 오류가 발생했습니다' });
    }
  };

  const sendBatchMessages = async () => {
    const validMessages = batchMessages.filter((msg) => msg.messageBody.trim());
    if (validMessages.length === 0) {
      setMessage({ type: 'error', text: '전송할 메시지를 입력해주세요' });
      return;
    }

    try {
      const response = await sqsApi.sendMessageBatch(queueName, { messages: validMessages });
      if (response.success && response.data) {
        const { successful, failed } = response.data;
        setMessage({
          type: 'success',
          text: `일괄 전송 완료: 성공 ${successful.length}개, 실패 ${failed.length}개`,
        });
      } else {
        setMessage({ type: 'error', text: response.error?.message || '일괄 전송 실패' });
      }
    } catch (error) {
      setMessage({ type: 'error', text: '일괄 전송 중 오류가 발생했습니다' });
    }
  };

  const deleteMessage = async (receiptHandle: string) => {
    try {
      const response = await sqsApi.deleteMessage(queueName, receiptHandle);
      if (response.success) {
        setMessage({ type: 'success', text: '메시지가 삭제되었습니다' });
        setMessages(messages.filter((msg) => msg.receiptHandle !== receiptHandle));
      } else {
        setMessage({ type: 'error', text: response.error?.message || '메시지 삭제 실패' });
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
      const response = await sqsApi.deleteMessageBatch(queueName, {
        receiptHandles: selectedMessages,
      });
      if (response.success && response.data) {
        const { successful, failed } = response.data;
        setMessage({
          type: 'success',
          text: `일괄 삭제 완료: 성공 ${successful.length}개, 실패 ${failed.length}개`,
        });
        setMessages(messages.filter((msg) => !selectedMessages.includes(msg.receiptHandle)));
        setSelectedMessages([]);
      } else {
        setMessage({ type: 'error', text: response.error?.message || '일괄 삭제 실패' });
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
      const response = await sqsApi.purgeQueue(queueName);
      if (response.success) {
        setMessage({ type: 'success', text: '큐의 모든 메시지가 삭제되었습니다' });
        setMessages([]);
      } else {
        setMessage({ type: 'error', text: response.error?.message || '큐 퍼지 실패' });
      }
    } catch (error) {
      setMessage({ type: 'error', text: '큐 퍼지 중 오류가 발생했습니다' });
    }
  };

  const addBatchMessage = () => {
    setBatchMessages([
      ...batchMessages,
      { id: `msg${batchMessages.length + 1}`, messageBody: '', delaySeconds: 0 },
    ]);
  };

  const removeBatchMessage = (index: number) => {
    setBatchMessages(batchMessages.filter((_, i) => i !== index));
  };

  const updateBatchMessage = (index: number, field: keyof BatchMessageRequest, value: any) => {
    const updated = [...batchMessages];
    updated[index] = { ...updated[index], [field]: value };
    setBatchMessages(updated);
  };

  useEffect(() => {
    if (message) {
      const timer = setTimeout(() => setMessage(null), 3000);
      return () => clearTimeout(timer);
    }
  }, [message]);

  return (
    <div className="message-manager">
      {message && (
        <div className={`message ${message.type}`}>
          {message.text}
        </div>
      )}

      {/* 단일 메시지 전송 */}
      <div className="form-section">
        <h4>단일 메시지 전송</h4>
        <div className="form-group">
          <label>메시지 내용</label>
          <textarea
            value={singleMessage.body}
            onChange={(e) => setSingleMessage({ ...singleMessage, body: e.target.value })}
            placeholder="메시지 내용을 입력하세요"
          />
        </div>
        <div className="form-group">
          <label>지연 시간 (초)</label>
          <input
            type="number"
            value={singleMessage.delaySeconds}
            onChange={(e) =>
              setSingleMessage({ ...singleMessage, delaySeconds: parseInt(e.target.value) || 0 })
            }
            placeholder="0"
          />
        </div>
        <div className="form-group">
          <label>속성 (JSON)</label>
          <input
            type="text"
            value={singleMessage.attributes}
            onChange={(e) => setSingleMessage({ ...singleMessage, attributes: e.target.value })}
            placeholder='{"key": "value"}'
          />
        </div>
        <button onClick={sendSingleMessage} className="btn btn-primary">
          메시지 전송
        </button>
      </div>

      {/* 일괄 메시지 전송 */}
      <div className="form-section">
        <h4>일괄 메시지 전송</h4>
        {batchMessages.map((msg, index) => (
          <div key={index} className="batch-message-item">
            <div className="form-group">
              <label>메시지 {index + 1}</label>
              <input
                type="text"
                value={msg.messageBody}
                onChange={(e) => updateBatchMessage(index, 'messageBody', e.target.value)}
                placeholder={`메시지 ${index + 1} 내용`}
              />
            </div>
            <div className="form-group">
              <label>지연 시간 (초)</label>
              <input
                type="number"
                value={msg.delaySeconds}
                onChange={(e) =>
                  updateBatchMessage(index, 'delaySeconds', parseInt(e.target.value) || 0)
                }
                placeholder="0"
              />
            </div>
            {batchMessages.length > 1 && (
              <button
                onClick={() => removeBatchMessage(index)}
                className="btn btn-danger btn-sm"
              >
                삭제
              </button>
            )}
          </div>
        ))}
        <div className="batch-controls">
          <button onClick={addBatchMessage} className="btn btn-secondary">
            메시지 추가
          </button>
          <button onClick={sendBatchMessages} className="btn btn-primary">
            일괄 전송
          </button>
        </div>
      </div>

      {/* 메시지 수신 및 관리 */}
      <div className="messages-section">
        <div className="form-row">
          <h4>수신된 메시지</h4>
          <div style={{ display: 'flex', gap: '0.5rem' }}>
            <button
              onClick={receiveMessages}
              disabled={loading}
              className="btn btn-success"
            >
              {loading ? '수신중...' : '메시지 수신'}
            </button>
            <button onClick={deleteSelectedMessages} className="btn btn-warning">
              선택 삭제
            </button>
            <button onClick={purgeQueue} className="btn btn-danger">
              큐 퍼지
            </button>
          </div>
        </div>

        {messages.length === 0 ? (
          <div className="empty-state">수신된 메시지가 없습니다.</div>
        ) : (
          <div>
            {messages.map((msg, index) => (
              <div key={msg.receiptHandle} className="message-item">
                <div className="message-header">
                  <div className="checkbox-item">
                    <input
                      type="checkbox"
                      id={`msg-${index}`}
                      onChange={(e) => {
                        if (e.target.checked) {
                          setSelectedMessages([...selectedMessages, msg.receiptHandle]);
                        } else {
                          setSelectedMessages(selectedMessages.filter(h => h !== msg.receiptHandle));
                        }
                      }}
                    />
                    <span className="message-id">ID: {msg.messageId}</span>
                  </div>
                  <div className="message-actions">
                    <button
                      onClick={() => deleteMessage(msg.receiptHandle)}
                      className="btn btn-danger btn-sm"
                    >
                      삭제
                    </button>
                  </div>
                </div>
                
                <div className="message-body">{msg.messageBody}</div>
                
                {Object.keys(msg.attributes).length > 0 && (
                  <div>
                    <strong>속성:</strong>
                    <div className="message-body" style={{ marginTop: '0.25rem' }}>
                      {JSON.stringify(msg.attributes, null, 2)}
                    </div>
                  </div>
                )}
                
                <div className="message-meta">
                  <span>전송 시간: {new Date(msg.sentTimestamp).toLocaleString()}</span>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
};

export default MessageManager;
