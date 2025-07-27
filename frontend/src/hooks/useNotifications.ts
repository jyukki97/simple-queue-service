import { useState, useCallback } from 'react';
import { NotificationType } from '../components/NotificationToast';

interface Notification {
  id: string;
  type: NotificationType;
  message: string;
  duration?: number;
  autoClose?: boolean;
}

export const useNotifications = () => {
  const [notifications, setNotifications] = useState<Notification[]>([]);

  const addNotification = useCallback((
    type: NotificationType,
    message: string,
    options: { duration?: number; autoClose?: boolean } = {}
  ) => {
    const id = Date.now().toString() + Math.random().toString(36).substr(2, 9);
    const notification: Notification = {
      id,
      type,
      message,
      duration: options.duration ?? 5000,
      autoClose: options.autoClose ?? true,
    };

    setNotifications(prev => [...prev, notification]);
    return id;
  }, []);

  const removeNotification = useCallback((id: string) => {
    setNotifications(prev => prev.filter(n => n.id !== id));
  }, []);

  const clearAllNotifications = useCallback(() => {
    setNotifications([]);
  }, []);

  // Convenience methods
  const showSuccess = useCallback((message: string, options?: { duration?: number; autoClose?: boolean }) => {
    return addNotification('success', message, options);
  }, [addNotification]);

  const showError = useCallback((message: string, options?: { duration?: number; autoClose?: boolean }) => {
    return addNotification('error', message, { duration: 8000, ...options });
  }, [addNotification]);

  const showWarning = useCallback((message: string, options?: { duration?: number; autoClose?: boolean }) => {
    return addNotification('warning', message, { duration: 6000, ...options });
  }, [addNotification]);

  const showInfo = useCallback((message: string, options?: { duration?: number; autoClose?: boolean }) => {
    return addNotification('info', message, options);
  }, [addNotification]);

  return {
    notifications,
    addNotification,
    removeNotification,
    clearAllNotifications,
    showSuccess,
    showError,
    showWarning,
    showInfo,
  };
};