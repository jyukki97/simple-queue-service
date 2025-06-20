import React, { useState, useEffect } from 'react';
import storageService, { 
  StorageMessagesSummaryResponse, 
  TierMessageInfo, 
  TierMessagesResponse,
  MessageLocationResponse 
} from '../services/storageService';

interface StorageMessagesViewProps {
  queueName: string;
  visible: boolean;
  onClose: () => void;
}

const StorageMessagesView: React.FC<StorageMessagesViewProps> = ({
  queueName,
  visible,
  onClose
}) => {
  const [loading, setLoading] = useState(false);
  const [summary, setSummary] = useState<StorageMessagesSummaryResponse | null>(null);
  const [selectedTier, setSelectedTier] = useState<string>('HOT');
  const [tierMessages, setTierMessages] = useState<TierMessagesResponse | null>(null);
  const [selectedMessage, setSelectedMessage] = useState<TierMessageInfo | null>(null);
  const [messageDetailVisible, setMessageDetailVisible] = useState(false);
  const [messageLocation, setMessageLocation] = useState<MessageLocationResponse | null>(null);
  const [error, setError] = useState<string>('');

  const loadMessagesSummary = React.useCallback(async () => {
    setLoading(true);
    setError('');
    try {
      const data = await storageService.getMessagesSummaryByTier(queueName);
      setSummary(data);
    } catch (error: any) {
      setError('스토리지 메시지 요약을 불러오는 중 오류가 발생했습니다: ' + error.message);
      console.error('Failed to load messages summary:', error);
    } finally {
      setLoading(false);
    }
  }, [queueName]);

  const loadTierMessages = React.useCallback(async (tier: string, offset: number = 0) => {
    setLoading(true);
    setError('');
    try {
      const data = await storageService.getMessagesByTier(queueName, tier, 20, offset);
      setTierMessages(data);
    } catch (error: any) {
      setError(`${storageService.getTierDisplayName(tier)} 메시지를 불러오는 중 오류가 발생했습니다: ` + error.message);
      console.error('Failed to load tier messages:', error);
    } finally {
      setLoading(false);
    }
  }, [queueName]);

  useEffect(() => {
    if (visible) {
      loadMessagesSummary();
    }
  }, [visible, loadMessagesSummary]);

  useEffect(() => {
    if (visible && selectedTier) {
      loadTierMessages(selectedTier);
    }
  }, [selectedTier, visible, loadTierMessages]);

  const handleMessageClick = async (messageInfo: TierMessageInfo) => {
    setSelectedMessage(messageInfo);
    try {
      const location = await storageService.getMessageLocation(messageInfo.messageId);
      setMessageLocation(location);
    } catch (error) {
      console.error('Failed to load message location:', error);
    }
    setMessageDetailVisible(true);
  };

  const handlePromoteMessage = async (messageId: string, targetTier: string) => {
    try {
      await storageService.promoteMessage(messageId, {
        targetTier,
        reason: 'Manual promotion from UI'
      });
      alert('메시지가 성공적으로 승격되었습니다');
      setMessageDetailVisible(false);
      // 데이터 새로고침
      loadMessagesSummary();
      loadTierMessages(selectedTier);
    } catch (error: any) {
      alert('메시지 승격 중 오류가 발생했습니다: ' + error.message);
      console.error('Failed to promote message:', error);
    }
  };

  const formatTimestamp = (timestamp: string) => {
    return new Date(timestamp).toLocaleString('ko-KR');
  };

  const renderMessageList = () => {
    if (!tierMessages || tierMessages.messages.length === 0) {
      return (
        <div style={{ textAlign: 'center', padding: '20px', color: '#666' }}>
          {storageService.getTierDisplayName(selectedTier)}에 메시지가 없습니다
        </div>
      );
    }

    return (
      <div>
        {tierMessages.messages.map((messageInfo) => (
          <div 
            key={messageInfo.messageId}
            style={{
              border: '1px solid #ddd',
              borderRadius: '4px',
              padding: '12px',
              marginBottom: '8px',
              cursor: 'pointer',
              backgroundColor: '#f9f9f9'
            }}
            onClick={() => handleMessageClick(messageInfo)}
          >
            <div style={{ fontWeight: 'bold' }}>Message ID: {messageInfo.messageId}</div>
            <div style={{ fontSize: '0.9em', color: '#666', marginTop: '4px' }}>
              <div>전송 시간: {formatTimestamp(messageInfo.sentTimestamp)}</div>
              <div>수신 횟수: {messageInfo.receivedCount}</div>
              <div>크기: {storageService.formatBytes(messageInfo.sizeBytes)}</div>
              {messageInfo.movedToTierAt && (
                <div>계층 이관 시간: {formatTimestamp(messageInfo.movedToTierAt)}</div>
              )}
            </div>
            <div style={{ marginTop: '8px', maxHeight: '60px', overflow: 'hidden' }}>
              {messageInfo.messageBody.length > 100 
                ? messageInfo.messageBody.substring(0, 100) + '...'
                : messageInfo.messageBody
              }
            </div>
          </div>
        ))}
      </div>
    );
  };

  const renderTabs = () => {
    if (!summary) return null;

    const tiers = [
      { key: 'HOT', summary: summary.hotMessages },
      { key: 'WARM', summary: summary.warmMessages },
      { key: 'COLD', summary: summary.coldMessages }
    ];

    return (
      <div>
        <div style={{ display: 'flex', marginBottom: '16px' }}>
          {tiers.map(({ key, summary: tierSummary }) => (
            <button
              key={key}
              onClick={() => setSelectedTier(key)}
              style={{
                padding: '8px 16px',
                margin: '0 4px',
                backgroundColor: selectedTier === key ? storageService.getTierColor(key) : '#f0f0f0',
                color: selectedTier === key ? 'white' : 'black',
                border: 'none',
                borderRadius: '4px',
                cursor: 'pointer'
              }}
            >
              {storageService.getTierDisplayName(key)} ({tierSummary.messageCount})
            </button>
          ))}
        </div>
        
        <div style={{ marginBottom: '16px', padding: '12px', backgroundColor: '#f5f5f5', borderRadius: '4px' }}>
          <div>
            {(() => {
              const tierData = selectedTier === 'HOT' ? summary.hotMessages : 
                               selectedTier === 'WARM' ? summary.warmMessages : 
                               summary.coldMessages;
              return (
                <>
                  <strong>총 메시지: {tierData.messageCount.toLocaleString()}개</strong>
                  <br />
                  <span>총 크기: {storageService.formatBytes(tierData.totalSizeBytes)}</span>
                </>
              );
            })()}
          </div>
          <button
            onClick={() => loadTierMessages(selectedTier)}
            disabled={loading}
            style={{ marginTop: '8px', padding: '4px 8px' }}
          >
            {loading ? '로딩 중...' : '새로고침'}
          </button>
        </div>
        
        {loading ? (
          <div style={{ textAlign: 'center', padding: '20px' }}>로딩 중...</div>
        ) : (
          renderMessageList()
        )}
      </div>
    );
  };

  if (!visible) return null;

  return (
    <div style={{
      position: 'fixed',
      top: 0,
      left: 0,
      right: 0,
      bottom: 0,
      backgroundColor: 'rgba(0,0,0,0.5)',
      display: 'flex',
      justifyContent: 'center',
      alignItems: 'center',
      zIndex: 1000
    }}>
      <div style={{
        backgroundColor: 'white',
        borderRadius: '8px',
        padding: '20px',
        maxWidth: '90vw',
        maxHeight: '90vh',
        width: '800px',
        overflow: 'auto'
      }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '20px' }}>
          <h2>스토리지 계층별 메시지 - {queueName}</h2>
          <button
            onClick={onClose}
            style={{
              background: 'none',
              border: 'none',
              fontSize: '20px',
              cursor: 'pointer'
            }}
          >
            ✕
          </button>
        </div>
        
        {error && (
          <div style={{ 
            color: 'red', 
            backgroundColor: '#ffebee', 
            padding: '10px', 
            borderRadius: '4px',
            marginBottom: '16px'
          }}>
            {error}
          </div>
        )}
        
        {loading && !summary ? (
          <div style={{ textAlign: 'center', padding: '40px' }}>데이터를 불러오는 중...</div>
        ) : (
          summary && renderTabs()
        )}
      </div>

      {/* 메시지 상세 정보 모달 */}
      {messageDetailVisible && selectedMessage && (
        <div style={{
          position: 'fixed',
          top: 0,
          left: 0,
          right: 0,
          bottom: 0,
          backgroundColor: 'rgba(0,0,0,0.7)',
          display: 'flex',
          justifyContent: 'center',
          alignItems: 'center',
          zIndex: 1001
        }}>
          <div style={{
            backgroundColor: 'white',
            borderRadius: '8px',
            padding: '20px',
            maxWidth: '70vw',
            maxHeight: '70vh',
            width: '600px',
            overflow: 'auto'
          }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '20px' }}>
              <h3>메시지 상세 정보</h3>
              <button
                onClick={() => setMessageDetailVisible(false)}
                style={{
                  background: 'none',
                  border: 'none',
                  fontSize: '18px',
                  cursor: 'pointer'
                }}
              >
                ✕
              </button>
            </div>
            
            <div style={{ marginBottom: '16px' }}>
              <h4>기본 정보</h4>
              <div><strong>메시지 ID:</strong> {selectedMessage.messageId}</div>
              <div><strong>현재 계층:</strong> {storageService.getTierDisplayName(selectedMessage.tier)}</div>
              <div><strong>전송 시간:</strong> {formatTimestamp(selectedMessage.sentTimestamp)}</div>
              <div><strong>수신 횟수:</strong> {selectedMessage.receivedCount}</div>
              <div><strong>메시지 크기:</strong> {storageService.formatBytes(selectedMessage.sizeBytes)}</div>
              {selectedMessage.movedToTierAt && (
                <div><strong>계층 이관 시간:</strong> {formatTimestamp(selectedMessage.movedToTierAt)}</div>
              )}
              {selectedMessage.compressionType && (
                <div><strong>압축 타입:</strong> {selectedMessage.compressionType}</div>
              )}
            </div>

            {messageLocation && (
              <div style={{ marginBottom: '16px' }}>
                <h4>위치 정보</h4>
                <div><strong>생성 시간:</strong> {formatTimestamp(messageLocation.createdAt)}</div>
                <div><strong>마지막 이동:</strong> {formatTimestamp(messageLocation.lastMovedAt)}</div>
              </div>
            )}

            <div style={{ marginBottom: '16px' }}>
              <h4>메시지 내용</h4>
              <pre style={{ 
                background: '#f5f5f5', 
                padding: '12px', 
                borderRadius: '4px',
                whiteSpace: 'pre-wrap',
                wordBreak: 'break-word',
                maxHeight: '200px',
                overflow: 'auto'
              }}>
                {selectedMessage.messageBody}
              </pre>
            </div>

            {Object.keys(selectedMessage.attributes).length > 0 && (
              <div style={{ marginBottom: '16px' }}>
                <h4>메시지 속성</h4>
                {Object.entries(selectedMessage.attributes).map(([key, value]) => (
                  <div key={key}>
                    <strong>{key}:</strong> {value}
                  </div>
                ))}
              </div>
            )}
            
            <div style={{ display: 'flex', gap: '8px', justifyContent: 'flex-end' }}>
              <button onClick={() => setMessageDetailVisible(false)}>
                닫기
              </button>
              {selectedMessage.tier !== 'HOT' && (
                <button
                  onClick={() => handlePromoteMessage(selectedMessage.messageId, 'HOT')}
                  style={{
                    backgroundColor: '#1890ff',
                    color: 'white',
                    border: 'none',
                    padding: '8px 16px',
                    borderRadius: '4px',
                    cursor: 'pointer'
                  }}
                >
                  핫 스토리지로 승격
                </button>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default StorageMessagesView;
