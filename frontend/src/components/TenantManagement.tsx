import React, { useState, useEffect } from 'react';
import './TenantManagement.css';

interface Tenant {
  tenantId: string;
  name: string;
  apiKey: string;
  createdAt: string;
  isActive: boolean;
  queueCount: number;
  messageCount: number;
  dailyApiCalls: number;
  storageUsed: number;
  quota: {
    maxQueues: number;
    maxMessages: number;
    maxApiCalls: number;
    maxStorage: number;
  };
}

const TenantManagement: React.FC = () => {
  const [tenants, setTenants] = useState<Tenant[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [newTenant, setNewTenant] = useState({
    name: '',
    maxQueues: 100,
    maxMessages: 100000,
    maxApiCalls: 10000,
    maxStorage: 1024 * 1024 * 1024 // 1GB
  });

  useEffect(() => {
    fetchTenants();
  }, []);

  const fetchTenants = async () => {
    try {
      setLoading(true);
      // Mock data - 실제로는 API에서 가져와야 함
      const mockTenants: Tenant[] = [
        {
          tenantId: 'tenant-1',
          name: 'E-Commerce Platform',
          apiKey: 'key_1234567890abcdef',
          createdAt: '2024-01-10T10:00:00Z',
          isActive: true,
          queueCount: 5,
          messageCount: 12580,
          dailyApiCalls: 8450,
          storageUsed: 524288000, // 500MB
          quota: {
            maxQueues: 100,
            maxMessages: 100000,
            maxApiCalls: 10000,
            maxStorage: 1024 * 1024 * 1024
          }
        },
        {
          tenantId: 'tenant-2',
          name: 'Analytics Service',
          apiKey: 'key_abcdef1234567890',
          createdAt: '2024-01-12T14:30:00Z',
          isActive: true,
          queueCount: 3,
          messageCount: 8920,
          dailyApiCalls: 5670,
          storageUsed: 314572800, // 300MB
          quota: {
            maxQueues: 50,
            maxMessages: 50000,
            maxApiCalls: 5000,
            maxStorage: 512 * 1024 * 1024
          }
        },
        {
          tenantId: 'tenant-3',
          name: 'Notification System',
          apiKey: 'key_fedcba0987654321',
          createdAt: '2024-01-08T09:15:00Z',
          isActive: false,
          queueCount: 2,
          messageCount: 3420,
          dailyApiCalls: 1250,
          storageUsed: 104857600, // 100MB
          quota: {
            maxQueues: 25,
            maxMessages: 25000,
            maxApiCalls: 2500,
            maxStorage: 256 * 1024 * 1024
          }
        }
      ];
      
      setTenants(mockTenants);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : '테넌트 목록을 불러오는데 실패했습니다.');
    } finally {
      setLoading(false);
    }
  };

  const handleCreateTenant = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      const tenantId = `tenant-${Date.now()}`;
      const apiKey = `key_${Math.random().toString(36).substring(2)}`;
      
      const createdTenant: Tenant = {
        tenantId,
        name: newTenant.name,
        apiKey,
        createdAt: new Date().toISOString(),
        isActive: true,
        queueCount: 0,
        messageCount: 0,
        dailyApiCalls: 0,
        storageUsed: 0,
        quota: {
          maxQueues: newTenant.maxQueues,
          maxMessages: newTenant.maxMessages,
          maxApiCalls: newTenant.maxApiCalls,
          maxStorage: newTenant.maxStorage
        }
      };
      
      setTenants([...tenants, createdTenant]);
      setShowCreateForm(false);
      setNewTenant({
        name: '',
        maxQueues: 100,
        maxMessages: 100000,
        maxApiCalls: 10000,
        maxStorage: 1024 * 1024 * 1024
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : '테넌트 생성에 실패했습니다.');
    }
  };

  const handleToggleStatus = async (tenantId: string) => {
    try {
      setTenants(tenants.map(tenant => 
        tenant.tenantId === tenantId 
          ? { ...tenant, isActive: !tenant.isActive }
          : tenant
      ));
    } catch (err) {
      setError(err instanceof Error ? err.message : '테넌트 상태 변경에 실패했습니다.');
    }
  };

  const formatBytes = (bytes: number): string => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  const formatNumber = (num: number): string => {
    return new Intl.NumberFormat().format(num);
  };

  const getUsagePercentage = (used: number, max: number): number => {
    return Math.round((used / max) * 100);
  };

  const getUsageColor = (percentage: number): string => {
    if (percentage >= 90) return '#ef4444';
    if (percentage >= 70) return '#f59e0b';
    return '#10b981';
  };

  if (loading) {
    return (
      <div className="tenant-management loading">
        <div className="spinner"></div>
        <p>테넌트 목록을 불러오는 중...</p>
      </div>
    );
  }

  return (
    <div className="tenant-management">
      <div className="tenant-header">
        <h1>👥 테넌트 관리</h1>
        <button 
          className="create-btn"
          onClick={() => setShowCreateForm(true)}
        >
          ➕ 새 테넌트 생성
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
            <h2>새 테넌트 생성</h2>
            <form onSubmit={handleCreateTenant}>
              <div className="form-group">
                <label>테넌트 이름:</label>
                <input
                  type="text"
                  value={newTenant.name}
                  onChange={(e) => setNewTenant({...newTenant, name: e.target.value})}
                  placeholder="테넌트 이름을 입력하세요"
                  required
                />
              </div>

              <div className="form-group">
                <label>최대 큐 수:</label>
                <input
                  type="number"
                  value={newTenant.maxQueues}
                  onChange={(e) => setNewTenant({...newTenant, maxQueues: Number(e.target.value)})}
                  min="1"
                  max="1000"
                />
              </div>

              <div className="form-group">
                <label>최대 메시지 수:</label>
                <input
                  type="number"
                  value={newTenant.maxMessages}
                  onChange={(e) => setNewTenant({...newTenant, maxMessages: Number(e.target.value)})}
                  min="1000"
                  max="10000000"
                />
              </div>

              <div className="form-group">
                <label>일일 API 호출 한도:</label>
                <input
                  type="number"
                  value={newTenant.maxApiCalls}
                  onChange={(e) => setNewTenant({...newTenant, maxApiCalls: Number(e.target.value)})}
                  min="100"
                  max="1000000"
                />
              </div>

              <div className="form-group">
                <label>스토리지 한도 (GB):</label>
                <input
                  type="number"
                  value={newTenant.maxStorage / (1024 * 1024 * 1024)}
                  onChange={(e) => setNewTenant({...newTenant, maxStorage: Number(e.target.value) * 1024 * 1024 * 1024})}
                  min="0.1"
                  max="1000"
                  step="0.1"
                />
              </div>

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

      <div className="tenant-grid">
        {tenants.map((tenant) => (
          <div key={tenant.tenantId} className={`tenant-card ${!tenant.isActive ? 'inactive' : ''}`}>
            <div className="tenant-card-header">
              <div className="tenant-info">
                <h3>{tenant.name}</h3>
                <p className="tenant-id">{tenant.tenantId}</p>
              </div>
              <div className="tenant-status">
                <button 
                  className={`status-toggle ${tenant.isActive ? 'active' : 'inactive'}`}
                  onClick={() => handleToggleStatus(tenant.tenantId)}
                >
                  {tenant.isActive ? '활성' : '비활성'}
                </button>
              </div>
            </div>

            <div className="tenant-details">
              <div className="detail-item">
                <span className="label">API 키:</span>
                <span className="api-key">{tenant.apiKey}</span>
                <button className="copy-btn" title="복사">📋</button>
              </div>
              <div className="detail-item">
                <span className="label">생성일:</span>
                <span className="value">{new Date(tenant.createdAt).toLocaleDateString('ko-KR')}</span>
              </div>
            </div>

            <div className="usage-stats">
              <h4>사용량 통계</h4>
              
              <div className="stat-item">
                <div className="stat-header">
                  <span className="stat-label">큐 수</span>
                  <span className="stat-value">
                    {formatNumber(tenant.queueCount)} / {formatNumber(tenant.quota.maxQueues)}
                  </span>
                </div>
                <div className="progress-bar">
                  <div 
                    className="progress-fill" 
                    style={{ 
                      width: `${getUsagePercentage(tenant.queueCount, tenant.quota.maxQueues)}%`,
                      backgroundColor: getUsageColor(getUsagePercentage(tenant.queueCount, tenant.quota.maxQueues))
                    }}
                  ></div>
                </div>
              </div>

              <div className="stat-item">
                <div className="stat-header">
                  <span className="stat-label">메시지 수</span>
                  <span className="stat-value">
                    {formatNumber(tenant.messageCount)} / {formatNumber(tenant.quota.maxMessages)}
                  </span>
                </div>
                <div className="progress-bar">
                  <div 
                    className="progress-fill" 
                    style={{ 
                      width: `${getUsagePercentage(tenant.messageCount, tenant.quota.maxMessages)}%`,
                      backgroundColor: getUsageColor(getUsagePercentage(tenant.messageCount, tenant.quota.maxMessages))
                    }}
                  ></div>
                </div>
              </div>

              <div className="stat-item">
                <div className="stat-header">
                  <span className="stat-label">일일 API 호출</span>
                  <span className="stat-value">
                    {formatNumber(tenant.dailyApiCalls)} / {formatNumber(tenant.quota.maxApiCalls)}
                  </span>
                </div>
                <div className="progress-bar">
                  <div 
                    className="progress-fill" 
                    style={{ 
                      width: `${getUsagePercentage(tenant.dailyApiCalls, tenant.quota.maxApiCalls)}%`,
                      backgroundColor: getUsageColor(getUsagePercentage(tenant.dailyApiCalls, tenant.quota.maxApiCalls))
                    }}
                  ></div>
                </div>
              </div>

              <div className="stat-item">
                <div className="stat-header">
                  <span className="stat-label">스토리지</span>
                  <span className="stat-value">
                    {formatBytes(tenant.storageUsed)} / {formatBytes(tenant.quota.maxStorage)}
                  </span>
                </div>
                <div className="progress-bar">
                  <div 
                    className="progress-fill" 
                    style={{ 
                      width: `${getUsagePercentage(tenant.storageUsed, tenant.quota.maxStorage)}%`,
                      backgroundColor: getUsageColor(getUsagePercentage(tenant.storageUsed, tenant.quota.maxStorage))
                    }}
                  ></div>
                </div>
              </div>
            </div>

            <div className="tenant-actions">
              <button className="action-btn">📊 상세 통계</button>
              <button className="action-btn">⚙️ 설정</button>
              <button className="action-btn">📝 로그</button>
            </div>
          </div>
        ))}
      </div>

      {tenants.length === 0 && !loading && (
        <div className="empty-state">
          <h3>생성된 테넌트가 없습니다</h3>
          <p>새 테넌트를 생성하여 서비스를 시작하세요.</p>
          <button 
            className="create-btn"
            onClick={() => setShowCreateForm(true)}
          >
            ➕ 첫 번째 테넌트 생성하기
          </button>
        </div>
      )}
    </div>
  );
};

export default TenantManagement;
