import React, { useState, useEffect, useCallback } from 'react';
import {
  StorageTier,
  TierMetrics,
  StorageUsageInfo,
  EnhancedMessageResponse,
} from '../types/sqs';
import { sqsApi } from '../services/sqsApi';
import './StorageTierManager.css';

interface StorageTierManagerProps {
  queueName?: string;
}

interface TierData {
  tier: StorageTier;
  metrics: TierMetrics | null;
  usage: StorageUsageInfo | null;
  messages: EnhancedMessageResponse[];
  loading: boolean;
}

const StorageTierManager: React.FC<StorageTierManagerProps> = ({ queueName }) => {
  const [tierData, setTierData] = useState<{ [key in StorageTier]: TierData }>({
    [StorageTier.HOT]: { tier: StorageTier.HOT, metrics: null, usage: null, messages: [], loading: false },
    [StorageTier.WARM]: { tier: StorageTier.WARM, metrics: null, usage: null, messages: [], loading: false },
    [StorageTier.COLD]: { tier: StorageTier.COLD, metrics: null, usage: null, messages: [], loading: false },
  });

  const [selectedTier, setSelectedTier] = useState<StorageTier>(StorageTier.HOT);
  const [message, setMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null);
  const [migrationInProgress, setMigrationInProgress] = useState(false);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [refreshInterval, setRefreshInterval] = useState(5000); // 5 seconds

  // Load tier data
  const loadTierData = useCallback(async (tier: StorageTier) => {
    setTierData(prev => ({
      ...prev,
      [tier]: { ...prev[tier], loading: true }
    }));

    try {
      const [metricsRes, usageRes, messagesRes] = await Promise.all([
        sqsApi.getTierMetrics(tier),
        sqsApi.getStorageUsage(tier),
        queueName ? sqsApi.getMessagesByTier(queueName, tier, 20) : Promise.resolve({ success: true, data: [] })
      ]);

      setTierData(prev => ({
        ...prev,
        [tier]: {
          ...prev[tier],
          metrics: metricsRes.success ? metricsRes.data! : null,
          usage: usageRes.success ? usageRes.data! : null,
          messages: messagesRes.success ? (messagesRes.data || []) : [],
          loading: false
        }
      }));
    } catch (error) {
      setTierData(prev => ({
        ...prev,
        [tier]: { ...prev[tier], loading: false }
      }));
      setMessage({ type: 'error', text: `Failed to load ${tier} tier data` });
    }
  }, [queueName]);

  // Load all tier data
  const loadAllTierData = useCallback(async () => {
    await Promise.all([
      loadTierData(StorageTier.HOT),
      loadTierData(StorageTier.WARM),
      loadTierData(StorageTier.COLD)
    ]);
  }, [loadTierData]);

  // Migration functions
  const migrateMessages = async (fromTier: StorageTier, toTier: StorageTier, messageIds?: string[]) => {
    setMigrationInProgress(true);
    try {
      const response = await sqsApi.migrateMessages(fromTier, toTier, messageIds);
      
      if (response.success && response.data) {
        setMessage({
          type: 'success',
          text: `Migration complete: ${response.data.successCount} messages moved from ${fromTier} to ${toTier}`
        });
        // Refresh affected tiers
        await Promise.all([loadTierData(fromTier), loadTierData(toTier)]);
      } else {
        setMessage({ type: 'error', text: response.error?.message || 'Migration failed' });
      }
    } catch (error) {
      setMessage({ type: 'error', text: 'Migration request failed' });
    } finally {
      setMigrationInProgress(false);
    }
  };

  // Format bytes to human readable
  const formatBytes = (bytes: number): string => {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
  };

  // Format milliseconds to human readable
  const formatDuration = (ms: number): string => {
    if (ms < 1000) return `${ms.toFixed(1)}ms`;
    if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
    return `${(ms / 60000).toFixed(1)}m`;
  };

  // Get tier color
  const getTierColor = (tier: StorageTier): string => {
    switch (tier) {
      case StorageTier.HOT: return '#dc3545';
      case StorageTier.WARM: return '#fd7e14';
      case StorageTier.COLD: return '#6f42c1';
      default: return '#6c757d';
    }
  };

  // Get performance status
  const getPerformanceStatus = (metrics: TierMetrics): 'excellent' | 'good' | 'poor' => {
    const { avgAccessTimeMs, hitRate, errorRate } = metrics;
    
    if (errorRate > 5) return 'poor';
    if (avgAccessTimeMs > 1000 || hitRate < 0.8) return 'poor';
    if (avgAccessTimeMs > 100 || hitRate < 0.95) return 'good';
    return 'excellent';
  };

  // Auto refresh effect
  useEffect(() => {
    if (autoRefresh) {
      const interval = setInterval(loadAllTierData, refreshInterval);
      return () => clearInterval(interval);
    }
  }, [autoRefresh, refreshInterval, queueName, loadAllTierData]);

  // Initial load
  useEffect(() => {
    loadAllTierData();
  }, [queueName, loadAllTierData]);

  // Message cleanup effect
  useEffect(() => {
    if (message) {
      const timer = setTimeout(() => setMessage(null), 5000);
      return () => clearTimeout(timer);
    }
  }, [message]);

  const currentTierData = tierData[selectedTier];

  return (
    <div className="storage-tier-manager">
      {message && (
        <div className={`message ${message.type}`}>
          {message.text}
        </div>
      )}

      {/* Controls */}
      <div className="tier-controls">
        <div className="control-group">
          <h3>Storage Tier Management</h3>
          <div className="refresh-controls">
            <label className="auto-refresh-label">
              <input
                type="checkbox"
                checked={autoRefresh}
                onChange={(e) => setAutoRefresh(e.target.checked)}
              />
              Auto Refresh
            </label>
            {autoRefresh && (
              <select
                value={refreshInterval}
                onChange={(e) => setRefreshInterval(parseInt(e.target.value))}
                className="form-control refresh-interval"
              >
                <option value={1000}>1s</option>
                <option value={5000}>5s</option>
                <option value={10000}>10s</option>
                <option value={30000}>30s</option>
              </select>
            )}
            <button onClick={loadAllTierData} className="btn btn-secondary btn-sm">
              Refresh Now
            </button>
          </div>
        </div>
      </div>

      {/* Tier Overview Cards */}
      <div className="tier-overview">
        {Object.values(StorageTier).map((tier) => {
          const data = tierData[tier];
          const isSelected = tier === selectedTier;
          
          return (
            <div
              key={tier}
              className={`tier-card ${isSelected ? 'selected' : ''} ${data.loading ? 'loading' : ''}`}
              onClick={() => setSelectedTier(tier)}
              style={{ borderColor: getTierColor(tier) }}
            >
              <div className="tier-header">
                <h4 style={{ color: getTierColor(tier) }}>{tier}</h4>
                {data.metrics && (
                  <span className={`status-badge ${getPerformanceStatus(data.metrics)}`}>
                    {getPerformanceStatus(data.metrics)}
                  </span>
                )}
              </div>

              {data.loading ? (
                <div className="loading-indicator">Loading...</div>
              ) : (
                <>
                  <div className="tier-metrics">
                    <div className="metric-item">
                      <span className="metric-label">Messages</span>
                      <span className="metric-value">
                        {data.metrics?.messageCount.toLocaleString() || '0'}
                      </span>
                    </div>
                    <div className="metric-item">
                      <span className="metric-label">Storage</span>
                      <span className="metric-value">
                        {data.usage ? formatBytes(data.usage.usedBytes) : '0 B'}
                      </span>
                    </div>
                    <div className="metric-item">
                      <span className="metric-label">Avg Access</span>
                      <span className="metric-value">
                        {data.metrics ? formatDuration(data.metrics.avgAccessTimeMs) : 'N/A'}
                      </span>
                    </div>
                  </div>

                  {data.usage && (
                    <div className="usage-bar">
                      <div
                        className="usage-fill"
                        style={{
                          width: `${Math.min(data.usage.usagePercent, 100)}%`,
                          backgroundColor: getTierColor(tier)
                        }}
                      />
                      <span className="usage-text">
                        {data.usage.usagePercent.toFixed(1)}% used
                      </span>
                    </div>
                  )}
                </>
              )}
            </div>
          );
        })}
      </div>

      {/* Detailed View */}
      <div className="tier-details">
        <div className="details-header">
          <h4>{selectedTier} Storage Details</h4>
          <div className="tier-actions">
            {selectedTier !== StorageTier.HOT && (
              <button
                onClick={() => migrateMessages(selectedTier, StorageTier.HOT)}
                disabled={migrationInProgress}
                className="btn btn-info btn-sm"
              >
                Promote to Hot
              </button>
            )}
            {selectedTier !== StorageTier.WARM && (
              <button
                onClick={() => migrateMessages(selectedTier, StorageTier.WARM)}
                disabled={migrationInProgress}
                className="btn btn-warning btn-sm"
              >
                Move to Warm
              </button>
            )}
            {selectedTier !== StorageTier.COLD && (
              <button
                onClick={() => migrateMessages(selectedTier, StorageTier.COLD)}
                disabled={migrationInProgress}
                className="btn btn-secondary btn-sm"
              >
                Archive to Cold
              </button>
            )}
          </div>
        </div>

        <div className="details-content">
          {/* Metrics Grid */}
          {currentTierData.metrics && (
            <div className="metrics-grid">
              <div className="metric-card">
                <div className="metric-title">Performance</div>
                <div className="metric-stats">
                  <div className="stat-row">
                    <span>Average Access Time</span>
                    <span>{formatDuration(currentTierData.metrics.avgAccessTimeMs)}</span>
                  </div>
                  <div className="stat-row">
                    <span>Hit Rate</span>
                    <span>{(currentTierData.metrics.hitRate * 100).toFixed(1)}%</span>
                  </div>
                  <div className="stat-row">
                    <span>Error Rate</span>
                    <span className={currentTierData.metrics.errorRate > 1 ? 'error' : 'success'}>
                      {currentTierData.metrics.errorRate.toFixed(2)}%
                    </span>
                  </div>
                </div>
              </div>

              <div className="metric-card">
                <div className="metric-title">Storage</div>
                <div className="metric-stats">
                  <div className="stat-row">
                    <span>Message Count</span>
                    <span>{currentTierData.metrics.messageCount.toLocaleString()}</span>
                  </div>
                  <div className="stat-row">
                    <span>Storage Usage</span>
                    <span>{formatBytes(currentTierData.metrics.storageUsageBytes)}</span>
                  </div>
                  <div className="stat-row">
                    <span>Migration Rate</span>
                    <span>{currentTierData.metrics.migrationRate.toFixed(2)}/min</span>
                  </div>
                </div>
              </div>

              {currentTierData.usage && (
                <div className="metric-card">
                  <div className="metric-title">Capacity</div>
                  <div className="metric-stats">
                    <div className="stat-row">
                      <span>Total Capacity</span>
                      <span>{formatBytes(currentTierData.usage.totalCapacityBytes)}</span>
                    </div>
                    <div className="stat-row">
                      <span>Available</span>
                      <span>{formatBytes(currentTierData.usage.availableBytes)}</span>
                    </div>
                    <div className="stat-row">
                      <span>Usage</span>
                      <span className={currentTierData.usage.usagePercent > 80 ? 'warning' : 'success'}>
                        {currentTierData.usage.usagePercent.toFixed(1)}%
                      </span>
                    </div>
                  </div>
                </div>
              )}

              <div className="metric-card">
                <div className="metric-title">Status</div>
                <div className="metric-stats">
                  <div className="stat-row">
                    <span>Overall Health</span>
                    <span className={`status ${getPerformanceStatus(currentTierData.metrics)}`}>
                      {getPerformanceStatus(currentTierData.metrics).toUpperCase()}
                    </span>
                  </div>
                  <div className="stat-row">
                    <span>Last Updated</span>
                    <span>{new Date(currentTierData.metrics.lastUpdated).toLocaleString()}</span>
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Messages List */}
          {queueName && (
            <div className="tier-messages">
              <div className="messages-header">
                <h5>Recent Messages in {selectedTier}</h5>
                <span className="message-count">
                  {currentTierData.messages.length} shown
                </span>
              </div>

              {currentTierData.messages.length === 0 ? (
                <div className="empty-state">
                  No messages found in {selectedTier} tier
                </div>
              ) : (
                <div className="messages-list">
                  {currentTierData.messages.map((message) => (
                    <div key={message.messageId} className="message-item">
                      <div className="message-header">
                        <div className="message-info">
                          <span className="message-id">{message.messageId}</span>
                          <span className="message-time">
                            {new Date(message.sentTimestamp).toLocaleString()}
                          </span>
                        </div>
                        <div className="message-actions">
                          {message.currentTier !== StorageTier.HOT && (
                            <button
                              onClick={() => migrateMessages(selectedTier, StorageTier.HOT, [message.messageId])}
                              disabled={migrationInProgress}
                              className="btn btn-info btn-xs"
                              title="Promote to Hot"
                            >
                              ↑ Hot
                            </button>
                          )}
                          {message.currentTier !== StorageTier.COLD && (
                            <button
                              onClick={() => migrateMessages(selectedTier, StorageTier.COLD, [message.messageId])}
                              disabled={migrationInProgress}
                              className="btn btn-secondary btn-xs"
                              title="Archive to Cold"
                            >
                              ↓ Cold
                            </button>
                          )}
                        </div>
                      </div>

                      <div className="message-body">
                        {message.messageBody.length > 100
                          ? `${message.messageBody.substring(0, 100)}...`
                          : message.messageBody
                        }
                      </div>

                      {message.migrationHistory && message.migrationHistory.length > 0 && (
                        <div className="migration-history">
                          <strong>Migration History:</strong>
                          {message.migrationHistory.slice(-3).map((migration, idx) => (
                            <span key={idx} className="migration-entry">
                              {migration.fromTier} → {migration.toTier} ({migration.reason})
                            </span>
                          ))}
                        </div>
                      )}

                      {message.accessCount !== undefined && (
                        <div className="message-meta">
                          <span>Access Count: {message.accessCount}</span>
                          {message.lastAccessTime && (
                            <span>Last Access: {new Date(message.lastAccessTime).toLocaleString()}</span>
                          )}
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default StorageTierManager;