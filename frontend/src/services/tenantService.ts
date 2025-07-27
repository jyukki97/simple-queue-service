import configService from '../utils/config';

export interface Tenant {
  tenantId: string;
  apiKey: string;
  name: string;
  email: string;
  status: 'ACTIVE' | 'SUSPENDED' | 'INACTIVE';
  quota: TenantQuota;
  createdAt: string;
  updatedAt: string;
}

// 백엔드 호환을 위한 별칭
export interface TenantResponse extends Tenant {}

// 확장된 테넌트 정보 (사용량 포함)
export interface ExtendedTenant extends Tenant {
  queueCount: number;
  messageCount: number;
  messagesProcessed?: number; // 처리된 메시지 수 추가
  dailyApiCalls: number;
  storageUsed: number;
  // 편의를 위한 isActive 속성 추가
  isActive: boolean;
}

export interface CreateTenantRequest {
  name: string;
  email: string;
  quota?: TenantQuota;
}

export interface UpdateTenantRequest {
  name?: string;
  email?: string;
  isActive?: boolean;
}

export interface TenantUsage {
  tenantId: string;
  queueCount: number;
  messageCount: number;
  messagesProcessedToday?: number; // 처리된 메시지 수 추가
  storageUsedBytes: number;
  apiCallsToday: number;
  lastActivity?: string;
}

export interface TenantQuota {
  maxQueues: number;
  maxMessagesPerQueue: number;
  maxMessageSizeKB: number;
  maxApiCallsPerMinute: number;
  // Optional fields for backward compatibility
  maxMessages?: number;
  maxApiCalls?: number;
  maxStorage?: number;
}

export interface ApiResponse<T> {
  success: boolean;
  data?: T;
  error?: {
    code: string;
    message: string;
  };
  timestamp: string;
}

class TenantService {
  private readonly apiConfig = configService.getApiConfig();

  private getHeaders() {
    return {
      'X-API-Key': this.apiConfig.key,
      'Content-Type': 'application/json'
    };
  }

  /**
   * 테넌트 목록 조회
   */
  async getTenants(): Promise<Tenant[]> {
    try {
      const response = await fetch('/api/v1/admin/tenants', {
        method: 'GET',
        headers: this.getHeaders()
      });

      if (!response.ok) {
        throw new Error(`HTTP Error: ${response.status}`);
      }

      const result: ApiResponse<any> = await response.json();
      
      if (!result.success) {
        throw new Error(result.error?.message || '테넌트 목록 조회에 실패했습니다.');
      }

      // 페이지네이션 응답에서 데이터 추출
      return result.data?.content || result.data || [];
    } catch (error) {
      console.error('Failed to fetch tenants:', error);
      // 에러 시 빈 배열 반환
      return [];
    }
  }

  /**
   * 테넌트 생성
   */
  async createTenant(request: CreateTenantRequest): Promise<Tenant> {
    try {
      console.log('Creating tenant with request:', JSON.stringify(request, null, 2));
      
      const response = await fetch('/api/v1/admin/tenants', {
        method: 'POST',
        headers: this.getHeaders(),
        body: JSON.stringify(request)
      });

      if (!response.ok) {
        const errorText = await response.text();
        console.error('Create tenant failed:', response.status, errorText);
        throw new Error(`HTTP Error: ${response.status}`);
      }

      const result: ApiResponse<Tenant> = await response.json();
      
      if (!result.success || !result.data) {
        throw new Error(result.error?.message || '테넌트 생성에 실패했습니다.');
      }

      return result.data;
    } catch (error) {
      console.error('Create tenant error:', error);
      throw new Error('테넌트 생성에 실패했습니다: ' + (error as Error).message);
    }
  }

  /**
   * 테넌트 수정
   */
  async updateTenant(tenantId: string, request: UpdateTenantRequest): Promise<Tenant> {
    try {
      const response = await fetch(`/api/v1/admin/tenants/${tenantId}`, {
        method: 'PUT',
        headers: this.getHeaders(),
        body: JSON.stringify(request)
      });

      if (!response.ok) {
        throw new Error(`HTTP Error: ${response.status}`);
      }

      const result: ApiResponse<Tenant> = await response.json();
      
      if (!result.success || !result.data) {
        throw new Error(result.error?.message || '테넌트 수정에 실패했습니다.');
      }

      return result.data;
    } catch (error) {
      throw new Error('테넌트 수정에 실패했습니다: ' + (error as Error).message);
    }
  }

  /**
   * 테넌트 삭제
   */
  async deleteTenant(tenantId: string): Promise<string> {
    try {
      const response = await fetch(`/api/v1/admin/tenants/${tenantId}`, {
        method: 'DELETE',
        headers: this.getHeaders()
      });

      if (!response.ok) {
        throw new Error(`HTTP Error: ${response.status}`);
      }

      const result: ApiResponse<any> = await response.json();
      
      if (!result.success) {
        throw new Error(result.error?.message || '테넌트 삭제에 실패했습니다.');
      }

      return `테넌트 ${tenantId}가 성공적으로 삭제되었습니다`;
    } catch (error) {
      throw new Error('테넌트 삭제에 실패했습니다: ' + (error as Error).message);
    }
  }

  /**
   * 테넌트 활성화
   */
  async activateTenant(tenantId: string): Promise<void> {
    try {
      const response = await fetch(`/api/v1/admin/tenants/${tenantId}/activate`, {
        method: 'POST',
        headers: this.getHeaders()
      });

      if (!response.ok) {
        throw new Error(`HTTP Error: ${response.status}`);
      }

      const result: ApiResponse<any> = await response.json();
      
      if (!result.success) {
        throw new Error(result.error?.message || '테넌트 활성화에 실패했습니다.');
      }
    } catch (error) {
      throw new Error('테넌트 활성화에 실패했습니다: ' + (error as Error).message);
    }
  }

  /**
   * 테넌트 일시중지
   */
  async suspendTenant(tenantId: string): Promise<void> {
    try {
      const response = await fetch(`/api/v1/admin/tenants/${tenantId}/suspend`, {
        method: 'POST',
        headers: this.getHeaders()
      });

      if (!response.ok) {
        throw new Error(`HTTP Error: ${response.status}`);
      }

      const result: ApiResponse<any> = await response.json();
      
      if (!result.success) {
        throw new Error(result.error?.message || '테넌트 일시중지에 실패했습니다.');
      }
    } catch (error) {
      throw new Error('테넌트 일시중지에 실패했습니다: ' + (error as Error).message);
    }
  }

  /**
   * 테넌트 사용량 통계 조회 (실제 데이터 사용)
   */
  async getTenantUsage(tenantId: string): Promise<TenantUsage> {
    try {
      // 실제 데이터를 가져오기 위해 대시보드 API 사용
      const response = await fetch('/api/v1/admin/dashboard', {
        method: 'GET',
        headers: this.getHeaders()
      });

      if (!response.ok) {
        throw new Error(`HTTP Error: ${response.status}`);
      }

      const result: ApiResponse<any> = await response.json();
      
      if (!result.success || !result.data) {
        throw new Error(result.error?.message || '사용량 통계 조회에 실패했습니다.');
      }

      // 대시보드 데이터에서 해당 테넌트 찾기
      const tenantStats = result.data.tenantUsageStats?.find((t: any) => t.tenantId === tenantId);
      
      if (tenantStats) {
        return {
          tenantId,
          queueCount: tenantStats.queueCount || 0,
          messageCount: tenantStats.messageCount || 0,
          messagesProcessedToday: tenantStats.messagesProcessedToday || 0,
          storageUsedBytes: tenantStats.storageUsed || 0,
          apiCallsToday: tenantStats.apiCallsToday || 0,
          lastActivity: tenantStats.lastActivity ? new Date(tenantStats.lastActivity * 1000).toISOString() : undefined
        };
      }

      // 테넌트를 찾지 못한 경우 기본값 반환
      return {
        tenantId,
        queueCount: 0,
        messageCount: 0,
        storageUsedBytes: 0,
        apiCallsToday: 0
      };
    } catch (error) {
      console.error('Failed to fetch tenant usage from dashboard:', error);
      // 에러 시 기본 값 반환
      return {
        tenantId,
        queueCount: 0,
        messageCount: 0,
        storageUsedBytes: 0,
        apiCallsToday: 0
      };
    }
  }

  /**
   * 테넌트 할당량 조회
   */
  async getTenantQuota(tenantId: string): Promise<TenantQuota> {
    try {
      const response = await fetch(`/api/v1/admin/tenants/${tenantId}/quota`, {
        method: 'GET',
        headers: this.getHeaders()
      });

      if (!response.ok) {
        throw new Error(`HTTP Error: ${response.status}`);
      }

      const result: ApiResponse<TenantQuota> = await response.json();
      
      if (!result.success || !result.data) {
        throw new Error(result.error?.message || '할당량 조회에 실패했습니다.');
      }

      return result.data;
    } catch (error) {
      console.error('Failed to fetch tenant quota:', error);
      // 에러 시 기본 값 반환
      return {
        maxQueues: 10,
        maxMessagesPerQueue: 1000,
        maxMessageSizeKB: 256,
        maxApiCallsPerMinute: 100
      };
    }
  }

  /**
   * 테넌트 API 키 재설정
   */
  async resetApiKey(tenantId: string): Promise<Tenant> {
    try {
      const response = await fetch(`/api/v1/admin/tenants/${tenantId}/reset-api-key`, {
        method: 'POST',
        headers: this.getHeaders()
      });

      if (!response.ok) {
        throw new Error(`HTTP Error: ${response.status}`);
      }

      const result: ApiResponse<Tenant> = await response.json();
      
      if (!result.success || !result.data) {
        throw new Error(result.error?.message || 'API 키 재설정에 실패했습니다.');
      }

      return result.data;
    } catch (error) {
      throw new Error('API 키 재설정에 실패했습니다: ' + (error as Error).message);
    }
  }
}

const tenantService = new TenantService();
export default tenantService;
