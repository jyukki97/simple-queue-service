import React, { memo } from 'react';
import { MessageResponse, MessagePreview } from '../types/sqs';

interface MessageListItemProps {
  message: MessageResponse | MessagePreview;
  isSelected?: boolean;
  onSelect?: (messageId: string) => void;
  onDelete?: (messageId: string, receiptHandle?: string) => void;
  showActions?: boolean;
}

const MessageListItem: React.FC<MessageListItemProps> = memo(({
  message,
  isSelected = false,
  onSelect,
  onDelete,
  showActions = true,
}) => {
  const formatTimestamp = (timestamp: number) => {
    return new Date(timestamp).toLocaleString('ko-KR');
  };

  const truncateBody = (body: string, maxLength: number = 100) => {
    return body.length > maxLength ? `${body.substring(0, maxLength)}...` : body;
  };

  const handleSelect = () => {
    if (onSelect) {
      onSelect(message.messageId);
    }
  };

  const handleDelete = () => {
    if (onDelete && 'receiptHandle' in message) {
      onDelete(message.messageId, message.receiptHandle);
    }
  };

  return (
    <div className={`message-item ${isSelected ? 'selected' : ''}`}>
      <div className="message-header">
        <input
          type="checkbox"
          checked={isSelected}
          onChange={handleSelect}
          className="message-checkbox"
        />
        <div className="message-id">
          <strong>ID:</strong> {message.messageId}
        </div>
        <div className="message-timestamp">
          {formatTimestamp(message.sentTimestamp)}
        </div>
      </div>
      
      <div className="message-body">
        <div className="message-content">
          {truncateBody(message.messageBody)}
        </div>
      </div>

      {Object.keys(message.attributes).length > 0 && (
        <div className="message-attributes">
          <strong>Attributes:</strong>
          <div className="attributes-list">
            {Object.entries(message.attributes).map(([key, value]) => (
              <span key={key} className="attribute-tag">
                {key}: {value}
              </span>
            ))}
          </div>
        </div>
      )}

      {'receiveCount' in message && (
        <div className="message-stats">
          <span className="receive-count">Received: {message.receiveCount} times</span>
          {'firstReceiveTimestamp' in message && message.firstReceiveTimestamp && (
            <span className="first-receive">
              First: {formatTimestamp(message.firstReceiveTimestamp)}
            </span>
          )}
        </div>
      )}

      {showActions && (
        <div className="message-actions">
          {'receiptHandle' in message && (
            <button
              onClick={handleDelete}
              className="delete-button"
              title="Delete message"
            >
              🗑️ Delete
            </button>
          )}
        </div>
      )}
    </div>
  );
});

MessageListItem.displayName = 'MessageListItem';

export default MessageListItem;