import React, { useState, useEffect } from 'react';
import { 
  LineChart, 
  Line, 
  XAxis, 
  YAxis, 
  CartesianGrid, 
  Tooltip, 
  Legend, 
  BarChart, 
  Bar, 
  ResponsiveContainer 
} from 'recharts';
import adminService from '../services/adminService';
import { DashboardData, SystemStatus } from '../types/admin';
import StreamingEvents from './StreamingEvents';
import { CONFIG } from '../utils/config';
import './Dashboard.css';

const Dashboard: React.FC = () => {
  const [dashboardData, setDashboardData] = useState<DashboardData | null>(null);
  const [systemStatus, setSystemStatus] = useState<SystemStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [refreshInterval, setRefreshInterval] = useState(30); // seconds

  useEffect(() => {
    fetchDashboardData();
    const interval = setInterval(fetchDashboardData, refreshInterval * 1000);
    return () => clearInterval(interval);
  }, [refreshInterval]);

  const fetchDashboardData = async () => {
    try {
      setLoading(true);
      const [dashboard, status] = await Promise.all([
        adminService.getDashboard(),
        adminService.getSystemStatus()
      ]);
      setDashboardData(dashboard);
      setSystemStatus(status);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : '데이터를 불러오는데 실패했습니다.');
    } finally {
      setLoading(false);
    }
  };

  const formatBytes = (bytes: number): string => {
    if (!bytes || bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(Math.abs(bytes)) / Math.log(k));
    const index = Math.min(i, sizes.length - 1);
    return parseFloat((bytes / Math.pow(k, index)).toFixed(2)) + ' ' + sizes[index];
  };

  const formatNumber = (num: number): string => {
    if (!num && num !== 0) return '0';
    return new Intl.NumberFormat('ko-KR').format(num);
  };

  const getStatusColor = (status: string): string => {
    switch (status?.toLowerCase()) {
      case 'healthy': return '#10B981';
      case 'warning': return '#F59E0B';
      case 'critical': return '#EF4444';
      default: return '#6B7280';
    }
  };

  if (loading && !dashboardData) {
    return (
      <div className="dashboard-loading">
        <div className="spinner"></div>
        <p>대시보드 데이터를 불러오는 중...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="dashboard-error">
        <h3>❌ 오류 발생</h3>
        <p>{error}</p>
        <button onClick={fetchDashboardData} className="retry-btn">
          다시 시도
        </button>
      </div>
    );
  }

  return (
    <div className="dashboard">
      <div className="dashboard-header">
        <h1>🎯 Simple Queue Service - 관리자 대시보드</h1>
        <div className="dashboard-controls">
          <select 
            value={refreshInterval} 
            onChange={(e) => setRefreshInterval(Number(e.target.value))}
            className="refresh-select"
          >
            <option value={10}>10초마다 새로고침</option>
            <option value={30}>30초마다 새로고침</option>
            <option value={60}>1분마다 새로고침</option>
            <option value={300}>5분마다 새로고침</option>
          </select>
          <button onClick={fetchDashboardData} className="refresh-btn" disabled={loading}>
            {loading ? '⏳' : '🔄'} 새로고침
          </button>
        </div>
      </div>

      {/* 시스템 상태 카드 */}
      {systemStatus && (
        <div className="status-cards">
          <div className="status-card">
            <div className="status-icon" style={{ backgroundColor: getStatusColor(systemStatus.overall) }}>
              {systemStatus.overall === 'healthy' ? '✅' : 
               systemStatus.overall === 'warning' ? '⚠️' : '❌'}
            </div>
            <div className="status-info">
              <h3>전체 시스템 상태</h3>
              <p className="status-value">{systemStatus.overall?.toUpperCase()}</p>
            </div>
          </div>

          <div className="status-card">
            <div className="status-icon redis">💾</div>
            <div className="status-info">
              <h3>Redis 상태</h3>
              <p className="status-value">{systemStatus.components?.redis}</p>
            </div>
          </div>

          <div className="status-card">
            <div className="status-icon storage">🗂️</div>
            <div className="status-info">
              <h3>스토리지 상태</h3>
              <p className="status-value">{systemStatus.components?.storage}</p>
            </div>
          </div>

          <div className="status-card">
            <div className="status-icon uptime">⏱️</div>
            <div className="status-info">
              <h3>업타임</h3>
              <p className="status-value">{systemStatus.uptime}</p>
            </div>
          </div>
        </div>
      )}

      {dashboardData && (
        <>
          {/* 주요 메트릭 */}
          <div className="metrics-overview">
            <div className="metric-card">
              <h3>📊 활성 테넌트</h3>
              <p className="metric-value">{formatNumber(dashboardData.totalTenants)}</p>
            </div>
            <div className="metric-card">
              <h3>📦 총 큐 수</h3>
              <p className="metric-value">{formatNumber(dashboardData.totalQueues)}</p>
            </div>
            <div className="metric-card">
              <h3>📨 총 메시지</h3>
              <p className="metric-value">{formatNumber(dashboardData.totalMessages)}</p>
            </div>
            <div className="metric-card">
              <h3>⚡ 처리된 메시지</h3>
              <p className="metric-value">{formatNumber(dashboardData.processedMessages)}</p>
            </div>
          </div>

          {/* 스토리지 사용량 */}
          <div className="storage-section">
            <h2>💾 스토리지 사용량</h2>
            <div className="storage-tiers">
              <div className="storage-tier hot">
                <h3>🔥 핫 스토리지</h3>
                <div className="storage-info">
                  <p>사용량: {formatBytes(dashboardData.storageUsage.hot.used)}</p>
                  <p>총 용량: {formatBytes(dashboardData.storageUsage.hot.total)}</p>
                  <div className="usage-bar">
                    <div 
                      className="usage-fill" 
                      style={{ 
                        width: `${Math.min(100, Math.max(0, (dashboardData.storageUsage.hot.used / dashboardData.storageUsage.hot.total) * 100))}%`,
                        backgroundColor: '#EF4444'
                      }}
                    ></div>
                  </div>
                </div>
              </div>

              <div className="storage-tier warm">
                <h3>🌡️ 웜 스토리지</h3>
                <div className="storage-info">
                  <p>사용량: {formatBytes(dashboardData.storageUsage.warm.used)}</p>
                  <p>총 용량: {formatBytes(dashboardData.storageUsage.warm.total)}</p>
                  <div className="usage-bar">
                    <div 
                      className="usage-fill" 
                      style={{ 
                        width: `${Math.min(100, Math.max(0, (dashboardData.storageUsage.warm.used / dashboardData.storageUsage.warm.total) * 100))}%`,
                        backgroundColor: '#F59E0B'
                      }}
                    ></div>
                  </div>
                </div>
              </div>

              <div className="storage-tier cold">
                <h3>❄️ 콜드 스토리지</h3>
                <div className="storage-info">
                  <p>사용량: {formatBytes(dashboardData.storageUsage.cold.used)}</p>
                  <p>총 용량: {formatBytes(dashboardData.storageUsage.cold.total)}</p>
                  <div className="usage-bar">
                    <div 
                      className="usage-fill" 
                      style={{ 
                        width: `${Math.min(100, Math.max(0, (dashboardData.storageUsage.cold.used / dashboardData.storageUsage.cold.total) * 100))}%`,
                        backgroundColor: '#3B82F6'
                      }}
                    ></div>
                  </div>
                </div>
              </div>
            </div>
          </div>

          {/* 메시지 처리량 차트 */}
          <div className="chart-section">
            <h2>📈 실시간 메시지 처리량</h2>
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={dashboardData.messageStats}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="timestamp" />
                <YAxis />
                <Tooltip />
                <Legend />
                <Line type="monotone" dataKey="sent" stroke="#10B981" name="전송된 메시지" />
                <Line type="monotone" dataKey="received" stroke="#3B82F6" name="수신된 메시지" />
                <Line type="monotone" dataKey="processed" stroke="#8B5CF6" name="처리된 메시지" />
              </LineChart>
            </ResponsiveContainer>
          </div>

          {/* 큐별 메트릭 */}
          <div className="queue-metrics-section">
            <h2>📦 상위 활성 큐</h2>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={dashboardData.topQueues}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="queueName" />
                <YAxis />
                <Tooltip />
                <Legend />
                <Bar dataKey="messageCount" fill="#10B981" name="메시지 수" />
                <Bar dataKey="processingRate" fill="#F59E0B" name="처리율" />
              </BarChart>
            </ResponsiveContainer>
          </div>

          {/* 에러 및 알림 */}
          {dashboardData.recentAlerts && dashboardData.recentAlerts.length > 0 && (
            <div className="alerts-section">
              <h2>🚨 최근 알림</h2>
              <div className="alerts-list">
                {dashboardData.recentAlerts.map((alert: any, index: number) => (
                  <div key={index} className={`alert-item ${alert.severity}`}>
                    <div className="alert-icon">
                      {alert.severity === 'critical' ? '🚨' : 
                       alert.severity === 'warning' ? '⚠️' : 'ℹ️'}
                    </div>
                    <div className="alert-content">
                      <h4>{alert.title}</h4>
                      <p>{alert.message}</p>
                      <span className="alert-time">{new Date(alert.timestamp).toLocaleString()}</span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* 실시간 이벤트 스트림 */}
          {CONFIG.FEATURES.enableStreaming && (
            <div className="streaming-section">
              <StreamingEvents maxEvents={50} />
            </div>
          )}
        </>
      )}
    </div>
  );
};

export default Dashboard;
