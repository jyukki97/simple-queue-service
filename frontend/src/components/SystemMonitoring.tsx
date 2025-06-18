import React, { useState, useEffect } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, AreaChart, Area } from 'recharts';
import './SystemMonitoring.css';

interface SystemMetric {
  timestamp: string;
  cpuUsage: number;
  memoryUsage: number;
  diskUsage: number;
  networkIn: number;
  networkOut: number;
  activeConnections: number;
  queueOperations: number;
  messageThroughput: number;
}

interface PerformanceAlert {
  id: string;
  type: 'cpu' | 'memory' | 'disk' | 'network' | 'queue';
  message: string;
  severity: 'warning' | 'critical';
  timestamp: string;
  value: number;
  threshold: number;
}

const SystemMonitoring: React.FC = () => {
  const [metrics, setMetrics] = useState<SystemMetric[]>([]);
  const [alerts, setAlerts] = useState<PerformanceAlert[]>([]);
  const [loading, setLoading] = useState(true);
  const [timeRange, setTimeRange] = useState<'1h' | '6h' | '24h' | '7d'>('1h');
  const [selectedMetric, setSelectedMetric] = useState<'system' | 'queue' | 'network'>('system');

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        // Mock data generation
        const now = new Date();
        const dataPoints = timeRange === '1h' ? 60 : timeRange === '6h' ? 72 : timeRange === '24h' ? 96 : 168;
        const interval = timeRange === '1h' ? 1 : timeRange === '6h' ? 5 : timeRange === '24h' ? 15 : 60;
        
        const mockMetrics: SystemMetric[] = [];
        for (let i = dataPoints; i >= 0; i--) {
          const timestamp = new Date(now.getTime() - i * interval * 60 * 1000);
          mockMetrics.push({
            timestamp: timestamp.toISOString(),
            cpuUsage: 20 + Math.random() * 60 + Math.sin(i / 10) * 10,
            memoryUsage: 40 + Math.random() * 40 + Math.sin(i / 15) * 15,
            diskUsage: 60 + Math.random() * 20,
            networkIn: Math.random() * 100 + 50,
            networkOut: Math.random() * 80 + 30,
            activeConnections: Math.floor(100 + Math.random() * 500),
            queueOperations: Math.floor(50 + Math.random() * 200),
            messageThroughput: Math.floor(500 + Math.random() * 1500)
          });
        }
        
        setMetrics(mockMetrics);
        
        // Mock alerts
        const mockAlerts: PerformanceAlert[] = [
          {
            id: 'alert-1',
            type: 'memory',
            message: '메모리 사용량이 높습니다',
            severity: 'warning',
            timestamp: new Date(Date.now() - 300000).toISOString(),
            value: 85,
            threshold: 80
          },
          {
            id: 'alert-2',
            type: 'queue',
            message: '큐 처리 지연이 발생했습니다',
            severity: 'critical',
            timestamp: new Date(Date.now() - 600000).toISOString(),
            value: 2500,
            threshold: 2000
          }
        ];
        
        setAlerts(mockAlerts);
      } catch (err) {
        console.error('Failed to fetch metrics:', err);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
    const interval = setInterval(fetchData, 30000); // 30초마다 업데이트
    return () => clearInterval(interval);
  }, [timeRange]);

  const formatTimestamp = (timestamp: string) => {
    const date = new Date(timestamp);
    if (timeRange === '1h' || timeRange === '6h') {
      return date.toLocaleTimeString('ko-KR', { hour: '2-digit', minute: '2-digit' });
    } else if (timeRange === '24h') {
      return date.toLocaleDateString('ko-KR', { month: 'short', day: 'numeric', hour: '2-digit' });
    } else {
      return date.toLocaleDateString('ko-KR', { month: 'short', day: 'numeric' });
    }
  };

  const getCurrentSystemStatus = () => {
    if (metrics.length === 0) return { status: 'unknown', color: '#6b7280' };
    
    const latest = metrics[metrics.length - 1];
    const avgCpu = latest.cpuUsage;
    const avgMemory = latest.memoryUsage;
    const avgDisk = latest.diskUsage;
    
    if (avgCpu > 90 || avgMemory > 90 || avgDisk > 95) {
      return { status: 'critical', color: '#ef4444' };
    } else if (avgCpu > 70 || avgMemory > 70 || avgDisk > 80) {
      return { status: 'warning', color: '#f59e0b' };
    } else {
      return { status: 'healthy', color: '#10b981' };
    }
  };

  const systemStatus = getCurrentSystemStatus();

  if (loading && metrics.length === 0) {
    return (
      <div className="system-monitoring loading">
        <div className="spinner"></div>
        <p>시스템 메트릭을 불러오는 중...</p>
      </div>
    );
  }

  return (
    <div className="system-monitoring">
      <div className="monitoring-header">
        <div className="header-left">
          <h1>📈 시스템 모니터링</h1>
          <div className="system-status">
            <div className="status-indicator" style={{ backgroundColor: systemStatus.color }}></div>
            <span className="status-text">
              시스템 상태: {systemStatus.status === 'healthy' ? '정상' : 
                          systemStatus.status === 'warning' ? '주의' : '위험'}
            </span>
          </div>
        </div>
        
        <div className="header-controls">
          <select 
            value={timeRange} 
            onChange={(e) => setTimeRange(e.target.value as any)}
            className="time-range-select"
          >
            <option value="1h">최근 1시간</option>
            <option value="6h">최근 6시간</option>
            <option value="24h">최근 24시간</option>
            <option value="7d">최근 7일</option>
          </select>
          
          <div className="metric-tabs">
            <button 
              className={`tab-btn ${selectedMetric === 'system' ? 'active' : ''}`}
              onClick={() => setSelectedMetric('system')}
            >
              시스템
            </button>
            <button 
              className={`tab-btn ${selectedMetric === 'queue' ? 'active' : ''}`}
              onClick={() => setSelectedMetric('queue')}
            >
              큐
            </button>
            <button 
              className={`tab-btn ${selectedMetric === 'network' ? 'active' : ''}`}
              onClick={() => setSelectedMetric('network')}
            >
              네트워크
            </button>
          </div>
        </div>
      </div>

      {/* 실시간 알림 */}
      {alerts.length > 0 && (
        <div className="alerts-section">
          <h2>🚨 활성 알림</h2>
          <div className="alerts-list">
            {alerts.map((alert) => (
              <div key={alert.id} className={`alert-item ${alert.severity}`}>
                <div className="alert-icon">
                  {alert.severity === 'critical' ? '🚨' : '⚠️'}
                </div>
                <div className="alert-content">
                  <div className="alert-header">
                    <span className="alert-type">{alert.type.toUpperCase()}</span>
                    <span className="alert-time">
                      {new Date(alert.timestamp).toLocaleTimeString('ko-KR')}
                    </span>
                  </div>
                  <p className="alert-message">{alert.message}</p>
                  <div className="alert-details">
                    현재 값: {alert.value} | 임계값: {alert.threshold}
                  </div>
                </div>
                <button className="dismiss-btn">✕</button>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* 메트릭 차트 */}
      <div className="charts-section">
        {selectedMetric === 'system' && (
          <>
            <div className="chart-container">
              <h3>💾 시스템 리소스 사용량</h3>
              <ResponsiveContainer width="100%" height={300}>
                <AreaChart data={metrics}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis 
                    dataKey="timestamp" 
                    tickFormatter={formatTimestamp}
                    interval="preserveStartEnd"
                  />
                  <YAxis domain={[0, 100]} />
                  <Tooltip 
                    labelFormatter={(value) => formatTimestamp(value as string)}
                    formatter={(value: number, name: string) => [`${value.toFixed(1)}%`, name]}
                  />
                  <Legend />
                  <Area 
                    type="monotone" 
                    dataKey="cpuUsage" 
                    stackId="1"
                    stroke="#ef4444" 
                    fill="#ef4444" 
                    name="CPU 사용률"
                    fillOpacity={0.6}
                  />
                  <Area 
                    type="monotone" 
                    dataKey="memoryUsage" 
                    stackId="2"
                    stroke="#3b82f6" 
                    fill="#3b82f6" 
                    name="메모리 사용률"
                    fillOpacity={0.6}
                  />
                  <Area 
                    type="monotone" 
                    dataKey="diskUsage" 
                    stackId="3"
                    stroke="#10b981" 
                    fill="#10b981" 
                    name="디스크 사용률"
                    fillOpacity={0.6}
                  />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </>
        )}

        {selectedMetric === 'queue' && (
          <>
            <div className="chart-container">
              <h3>📦 큐 작업 처리량</h3>
              <ResponsiveContainer width="100%" height={300}>
                <LineChart data={metrics}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis 
                    dataKey="timestamp" 
                    tickFormatter={formatTimestamp}
                    interval="preserveStartEnd"
                  />
                  <YAxis />
                  <Tooltip 
                    labelFormatter={(value) => formatTimestamp(value as string)}
                    formatter={(value: number, name: string) => [value.toFixed(0), name]}
                  />
                  <Legend />
                  <Line 
                    type="monotone" 
                    dataKey="queueOperations" 
                    stroke="#8b5cf6" 
                    strokeWidth={2}
                    name="큐 작업/분"
                  />
                  <Line 
                    type="monotone" 
                    dataKey="messageThroughput" 
                    stroke="#f59e0b" 
                    strokeWidth={2}
                    name="메시지 처리량/분"
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </>
        )}

        {selectedMetric === 'network' && (
          <>
            <div className="chart-container">
              <h3>🌐 네트워크 트래픽</h3>
              <ResponsiveContainer width="100%" height={300}>
                <AreaChart data={metrics}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis 
                    dataKey="timestamp" 
                    tickFormatter={formatTimestamp}
                    interval="preserveStartEnd"
                  />
                  <YAxis />
                  <Tooltip 
                    labelFormatter={(value) => formatTimestamp(value as string)}
                    formatter={(value: number, name: string) => [`${value.toFixed(1)} MB/s`, name]}
                  />
                  <Legend />
                  <Area 
                    type="monotone" 
                    dataKey="networkIn" 
                    stackId="1"
                    stroke="#10b981" 
                    fill="#10b981" 
                    name="인바운드"
                    fillOpacity={0.8}
                  />
                  <Area 
                    type="monotone" 
                    dataKey="networkOut" 
                    stackId="2"
                    stroke="#3b82f6" 
                    fill="#3b82f6" 
                    name="아웃바운드"
                    fillOpacity={0.8}
                  />
                </AreaChart>
              </ResponsiveContainer>
            </div>

            <div className="chart-container">
              <h3>🔗 활성 연결 수</h3>
              <ResponsiveContainer width="100%" height={200}>
                <LineChart data={metrics}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis 
                    dataKey="timestamp" 
                    tickFormatter={formatTimestamp}
                    interval="preserveStartEnd"
                  />
                  <YAxis />
                  <Tooltip 
                    labelFormatter={(value) => formatTimestamp(value as string)}
                    formatter={(value: number) => [value.toFixed(0), '연결 수']}
                  />
                  <Line 
                    type="monotone" 
                    dataKey="activeConnections" 
                    stroke="#6366f1" 
                    strokeWidth={2}
                    name="활성 연결"
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </>
        )}
      </div>

      {/* 시스템 정보 카드 */}
      <div className="info-cards">
        <div className="info-card">
          <h4>🖥️ 시스템 정보</h4>
          <div className="info-list">
            <div className="info-item">
              <span className="info-label">운영체제:</span>
              <span className="info-value">Linux Ubuntu 22.04</span>
            </div>
            <div className="info-item">
              <span className="info-label">Java 버전:</span>
              <span className="info-value">OpenJDK 21.0.1</span>
            </div>
            <div className="info-item">
              <span className="info-label">Spring Boot:</span>
              <span className="info-value">3.2.0</span>
            </div>
            <div className="info-item">
              <span className="info-label">Redis 버전:</span>
              <span className="info-value">7.0.15</span>
            </div>
          </div>
        </div>

        <div className="info-card">
          <h4>📊 실시간 통계</h4>
          <div className="info-list">
            {metrics.length > 0 && (
              <>
                <div className="info-item">
                  <span className="info-label">현재 CPU:</span>
                  <span className="info-value">{metrics[metrics.length - 1].cpuUsage.toFixed(1)}%</span>
                </div>
                <div className="info-item">
                  <span className="info-label">현재 메모리:</span>
                  <span className="info-value">{metrics[metrics.length - 1].memoryUsage.toFixed(1)}%</span>
                </div>
                <div className="info-item">
                  <span className="info-label">활성 연결:</span>
                  <span className="info-value">{metrics[metrics.length - 1].activeConnections}</span>
                </div>
                <div className="info-item">
                  <span className="info-label">메시지 처리량:</span>
                  <span className="info-value">{metrics[metrics.length - 1].messageThroughput}/분</span>
                </div>
              </>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

export default SystemMonitoring;
