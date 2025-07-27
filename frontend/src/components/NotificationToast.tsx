import React, { memo, useEffect, useState } from 'react';
import './NotificationToast.css';

export type NotificationType = 'success' | 'error' | 'warning' | 'info';

interface NotificationToastProps {
  type: NotificationType;
  message: string;
  duration?: number;
  onClose?: () => void;
  autoClose?: boolean;
}

const NotificationToast: React.FC<NotificationToastProps> = memo(({
  type,
  message,
  duration = 5000,
  onClose,
  autoClose = true,
}) => {
  const [isVisible, setIsVisible] = useState(false);
  const [isLeaving, setIsLeaving] = useState(false);

  useEffect(() => {
    // Trigger entrance animation
    const timer = setTimeout(() => setIsVisible(true), 10);
    return () => clearTimeout(timer);
  }, []);

  useEffect(() => {
    if (autoClose && duration > 0) {
      const timer = setTimeout(() => {
        handleClose();
      }, duration);

      return () => clearTimeout(timer);
    }
  }, [autoClose, duration]);

  const handleClose = () => {
    setIsLeaving(true);
    setTimeout(() => {
      if (onClose) {
        onClose();
      }
    }, 300); // Match the CSS transition duration
  };

  const getIcon = () => {
    switch (type) {
      case 'success':
        return '✅';
      case 'error':
        return '❌';
      case 'warning':
        return '⚠️';
      case 'info':
        return 'ℹ️';
      default:
        return 'ℹ️';
    }
  };

  const getTypeLabel = () => {
    switch (type) {
      case 'success':
        return '성공';
      case 'error':
        return '오류';
      case 'warning':
        return '경고';
      case 'info':
        return '정보';
      default:
        return '알림';
    }
  };

  return (
    <div 
      className={`notification-toast ${type} ${isVisible ? 'visible' : ''} ${isLeaving ? 'leaving' : ''}`}
      role="alert"
      aria-live="polite"
    >
      <div className="toast-content">
        <div className="toast-icon" aria-hidden="true">
          {getIcon()}
        </div>
        <div className="toast-body">
          <div className="toast-type" aria-label={`${getTypeLabel()} 알림`}>
            {getTypeLabel()}
          </div>
          <div className="toast-message">
            {message}
          </div>
        </div>
        <button 
          className="toast-close"
          onClick={handleClose}
          aria-label="알림 닫기"
          type="button"
        >
          ×
        </button>
      </div>
      
      {autoClose && duration > 0 && (
        <div 
          className="toast-progress"
          style={{ 
            animationDuration: `${duration}ms` 
          }}
        />
      )}
    </div>
  );
});

NotificationToast.displayName = 'NotificationToast';

export default NotificationToast;