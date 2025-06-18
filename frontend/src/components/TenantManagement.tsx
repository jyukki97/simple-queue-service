import React, { useState, useEffect } from 'react';
import tenantService, { ExtendedTenant, CreateTenantRequest } from '../services/tenantService';
import './TenantManagement.css';

const TenantManagement: React.FC = () => {
  const [tenants, setTenants] = useState<ExtendedTenant[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [showDetailModal, setShowDetailModal] = useState(false);
  const [selectedTenant, setSelectedTenant] = useState<ExtendedTenant | null>(null);
  const [newTenant, setNewTenant] = useState({
    name: '',
    email: '',
    maxQueues: 10,
    maxMessagesPerQueue: 1000,
    maxMessageSizeKB: 256,
    maxApiCallsPerMinute: 100
  });

  useEffect(() => {
    fetchTenants();
  }, []);

  const fetchTenants = async () => {
    try {
      setLoading(true);
      const tenantsData = await tenantService.getTenants();
      
      // 각 테넌트에 대한 추가 정보 수집
      const enrichedTenants = await Promise.all(
        tenantsData.map(async (tenant) => {
          try {
            const [usage, quota] = await Promise.all([
              tenantService.getTenantUsage(tenant.tenantId),
              tenantService.getTenantQuota(tenant.tenantId)
            ]);
            
            return {
              ...tenant,
              queueCount: usage.queueCount,
              messageCount: usage.messageCount,
              dailyApiCalls: usage.apiCallsToday,
              storageUsed: usage.storageUsedBytes,
              quota: {
                maxQueues: quota.maxQueues || 10,
                maxMessagesPerQueue: quota.maxMessagesPerQueue || 1000,
                maxMessageSizeKB: quota.maxMessageSizeKB || 256,
                maxApiCallsPerMinute: quota.maxApiCallsPerMinute || 100,
                // 백워드 호환을 위한 임시 매핑
                maxMessages: quota.maxMessagesPerQueue || 1000,
                maxApiCalls: quota.maxApiCallsPerMinute || 100,
                maxStorage: 1024 * 1024 * 1024 // 1GB 기본값
              },
              isActive: tenant.status === 'ACTIVE'
            } as ExtendedTenant;
          } catch (err) {
            console.error(`Error fetching data for tenant ${tenant.tenantId}:`, err);
            return {
              ...tenant,
              queueCount: 0,
              messageCount: 0,
              dailyApiCalls: 0,
              storageUsed: 0,
              quota: {
                maxQueues: 10,
                maxMessagesPerQueue: 1000,
                maxMessageSizeKB: 256,
                maxApiCallsPerMinute: 100,
                maxMessages: 1000,
                maxApiCalls: 100,
                maxStorage: 1024 * 1024 * 1024
              },
              isActive: tenant.status === 'ACTIVE'
            } as ExtendedTenant;
          }
        })
      );
      
      setTenants(enrichedTenants);
      setError(null);
    } catch (err) {
      console.error('Failed to fetch tenants:', err);
      setError(err instanceof Error ? err.message : '테넌트 목록을 불러오는데 실패했습니다.');
    } finally {
      setLoading(false);
    }
  };

  const handleCreateTenant = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      const request: CreateTenantRequest = {
        name: newTenant.name,
        email: newTenant.email,
        quota: {
          maxQueues: newTenant.maxQueues,
          maxMessagesPerQueue: newTenant.maxMessagesPerQueue,
          maxMessageSizeKB: newTenant.maxMessageSizeKB,
          maxApiCallsPerMinute: newTenant.maxApiCallsPerMinute
        }
      };
      
      await tenantService.createTenant(request);
      await fetchTenants(); // 목록 새로고침
      setShowCreateForm(false);
      setNewTenant({
        name: '',
        email: '',
        maxQueues: 10,
        maxMessagesPerQueue: 1000,
        maxMessageSizeKB: 256,
        maxApiCallsPerMinute: 100
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : '테넌트 생성에 실패했습니다.');
    }
  };

  const handleToggleStatus = async (tenantId: string, currentStatus: boolean) => {
    try {
      if (currentStatus) {
        await tenantService.suspendTenant(tenantId);
      } else {
        await tenantService.activateTenant(tenantId);
      }
      await fetchTenants(); // 목록 새로고침
    } catch (err) {
      setError(err instanceof Error ? err.message : '테넌트 상태 변경에 실패했습니다.');
    }
  };

  const handleDeleteTenant = async (tenantId: string) => {
    if (!window.confirm('정말로 이 테넌트를 삭제하시겠습니까? 이 작업은 되돌릴 수 없습니다.')) {
      return;
    }

    try {
      await tenantService.deleteTenant(tenantId);
      await fetchTenants(); // 목록 새로고침
    } catch (err) {
      setError(err instanceof Error ? err.message : '테넌트 삭제에 실패했습니다.');
    }
  };

  const handleResetApiKey = async (tenantId: string) => {
    if (!window.confirm('API 키를 재설정하시겠습니까? 기존 키는 더 이상 사용할 수 없습니다.')) {
      return;
    }

    try {
      await tenantService.resetApiKey(tenantId);
      await fetchTenants(); // 목록 새로고침
    } catch (err) {
      setError(err instanceof Error ? err.message : 'API 키 재설정에 실패했습니다.');
    }
  };

  const handleShowDetails = async (tenant: ExtendedTenant) => {
    setSelectedTenant(tenant);
    setShowDetailModal(true);
  };

  const copyToClipboard = async (text: string) => {
    try {
      await navigator.clipboard.writeText(text);
      alert('클립보드에 복사되었습니다.');
    } catch (err) {
      console.error('클립보드 복사 실패:', err);
      // 백업 방법
      const textArea = document.createElement('textarea');
      textArea.value = text;
      document.body.appendChild(textArea);
      textArea.select();
      document.execCommand('copy');
      document.body.removeChild(textArea);
      alert('클립보드에 복사되었습니다.');
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
                <label>이메일:</label>
                <input
                  type="email"
                  value={newTenant.email}
                  onChange={(e) => setNewTenant({...newTenant, email: e.target.value})}
                  placeholder="이메일을 입력하세요"
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
                  max="100"
                />
              </div>

              <div className="form-group">
                <label>큐당 최대 메시지 수:</label>
                <input
                  type="number"
                  value={newTenant.maxMessagesPerQueue}
                  onChange={(e) => setNewTenant({...newTenant, maxMessagesPerQueue: Number(e.target.value)})}
                  min="100"
                  max="10000"
                />
              </div>

              <div className="form-group">
                <label>분당 최대 API 호출:</label>
                <input
                  type="number"
                  value={newTenant.maxApiCallsPerMinute}
                  onChange={(e) => setNewTenant({...newTenant, maxApiCallsPerMinute: Number(e.target.value)})}
                  min="10"
                  max="1000"
                />
              </div>

              <div className="form-group">
                <label>최대 메시지 크기 (KB):</label>
                <input
                  type="number"
                  value={newTenant.maxMessageSizeKB}
                  onChange={(e) => setNewTenant({...newTenant, maxMessageSizeKB: Number(e.target.value)})}
                  min="1"
                  max="1024"
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
                  onClick={() => handleToggleStatus(tenant.tenantId, tenant.isActive)}
                >
                  {tenant.isActive ? '활성' : '비활성'}
                </button>
              </div>
            </div>

            <div className="tenant-details">
              <div className="detail-item">
                <span className="label">API 키:</span>
                <span className="api-key">{tenant.apiKey}</span>
                <button 
                  className="copy-btn" 
                  title="복사"
                  onClick={() => copyToClipboard(tenant.apiKey)}
                >
                  📋
                </button>
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
                    {formatNumber(tenant.messageCount)} / {formatNumber(tenant.quota.maxMessagesPerQueue)}
                  </span>
                </div>
                <div className="progress-bar">
                  <div 
                    className="progress-fill" 
                    style={{ 
                      width: `${getUsagePercentage(tenant.messageCount, tenant.quota.maxMessagesPerQueue)}%`,
                      backgroundColor: getUsageColor(getUsagePercentage(tenant.messageCount, tenant.quota.maxMessagesPerQueue))
                    }}
                  ></div>
                </div>
              </div>

              <div className="stat-item">
                <div className="stat-header">
                  <span className="stat-label">일일 API 호출</span>
                  <span className="stat-value">
                    {formatNumber(tenant.dailyApiCalls)} / {formatNumber(tenant.quota.maxApiCallsPerMinute)}
                  </span>
                </div>
                <div className="progress-bar">
                  <div 
                    className="progress-fill" 
                    style={{ 
                      width: `${getUsagePercentage(tenant.dailyApiCalls, tenant.quota.maxApiCallsPerMinute)}%`,
                      backgroundColor: getUsageColor(getUsagePercentage(tenant.dailyApiCalls, tenant.quota.maxApiCallsPerMinute))
                    }}
                  ></div>
                </div>
              </div>

              <div className="stat-item">
                <div className="stat-header">
                  <span className="stat-label">스토리지</span>
                  <span className="stat-value">
                    {formatBytes(tenant.storageUsed)} / {formatBytes(1024 * 1024 * 1024)}
                  </span>
                </div>
                <div className="progress-bar">
                  <div 
                    className="progress-fill" 
                    style={{ 
                      width: `${getUsagePercentage(tenant.storageUsed, 1024 * 1024 * 1024)}%`,
                      backgroundColor: getUsageColor(getUsagePercentage(tenant.storageUsed, 1024 * 1024 * 1024))
                    }}
                  ></div>
                </div>
              </div>
            </div>

            <div className="tenant-actions">
              <button 
                className="action-btn"
                onClick={() => handleShowDetails(tenant)}
              >
                📊 상세 통계
              </button>
              <button 
                className="action-btn"
                onClick={() => handleResetApiKey(tenant.tenantId)}
              >
                🔑 API 키 재설정
              </button>
              <button 
                className="action-btn delete-btn"
                onClick={() => handleDeleteTenant(tenant.tenantId)}
              >
                �️ 삭제
              </button>
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

      {/* 상세 통계 모달 */}
      {showDetailModal && selectedTenant && (
        <div className="modal-overlay">
          <div className="modal detail-modal">
            <h2>📊 {selectedTenant.name} - 상세 통계</h2>
            
            <div className="detail-stats-grid">
              <div className="detail-stat-card">
                <h3>기본 정보</h3>
                <div className="stat-row">
                  <span>테넌트 ID:</span>
                  <span>{selectedTenant.tenantId}</span>
                </div>
                <div className="stat-row">
                  <span>생성일:</span>
                  <span>{new Date(selectedTenant.createdAt).toLocaleDateString('ko-KR')}</span>
                </div>
                <div className="stat-row">
                  <span>상태:</span>
                  <span className={selectedTenant.isActive ? 'status-active' : 'status-inactive'}>
                    {selectedTenant.isActive ? '활성' : '비활성'}
                  </span>
                </div>
              </div>

              <div className="detail-stat-card">
                <h3>사용량 현황</h3>
                <div className="stat-row">
                  <span>큐 수:</span>
                  <span>{formatNumber(selectedTenant.queueCount)} / {formatNumber(selectedTenant.quota.maxQueues)}</span>
                </div>
                <div className="stat-row">
                  <span>메시지 수:</span>
                  <span>{formatNumber(selectedTenant.messageCount)} / {formatNumber(selectedTenant.quota.maxMessagesPerQueue)}</span>
                </div>
                <div className="stat-row">
                  <span>오늘 API 호출:</span>
                  <span>{formatNumber(selectedTenant.dailyApiCalls)} / {formatNumber(selectedTenant.quota.maxApiCallsPerMinute)}</span>
                </div>
                <div className="stat-row">
                  <span>스토리지:</span>
                  <span>{formatBytes(selectedTenant.storageUsed)} / {formatBytes(1024 * 1024 * 1024)}</span>
                </div>
              </div>

              <div className="detail-stat-card">
                <h3>API 키 관리</h3>
                <div className="api-key-section">
                  <label>현재 API 키:</label>
                  <div className="api-key-display">
                    <input 
                      type="text" 
                      value={selectedTenant.apiKey} 
                      readOnly 
                      className="api-key-input"
                    />
                    <button 
                      className="copy-btn"
                      onClick={() => copyToClipboard(selectedTenant.apiKey)}
                    >
                      📋
                    </button>
                  </div>
                  <button 
                    className="reset-key-btn"
                    onClick={() => {
                      handleResetApiKey(selectedTenant.tenantId);
                      setShowDetailModal(false);
                    }}
                  >
                    🔑 새 API 키 생성
                  </button>
                </div>
              </div>
            </div>

            <div className="modal-actions">
              <button 
                className="cancel-btn"
                onClick={() => setShowDetailModal(false)}
              >
                닫기
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default TenantManagement;
