/**
 * Application Configuration
 * Centralized configuration management with environment variable fallbacks
 */

interface AppConfig {
  api: {
    baseUrl: string;
    key: string;
    timeout: number;
  };
  websocket: {
    baseUrl: string;
    reconnectAttempts: number;
    reconnectDelay: number;
  };
  ui: {
    autoRefreshInterval: number;
    debugMode: boolean;
    enableConsoleLogs: boolean;
  };
  features: {
    enableAnalytics: boolean;
    enableErrorReporting: boolean;
    enableStreaming: boolean;
  };
}

class ConfigService {
  private config: AppConfig;

  constructor() {
    this.config = this.loadConfig();
    this.validateConfig();
  }

  private loadConfig(): AppConfig {
    const apiBaseUrl = process.env.REACT_APP_API_URL || 'http://localhost:8080';
    const wsBaseUrl = process.env.REACT_APP_WS_URL || apiBaseUrl.replace('http://', 'ws://').replace('https://', 'wss://');
    
    return {
      api: {
        baseUrl: apiBaseUrl,
        key: process.env.REACT_APP_API_KEY || this.getDefaultApiKey(),
        timeout: parseInt(process.env.REACT_APP_API_TIMEOUT || '10000', 10),
      },
      websocket: {
        baseUrl: wsBaseUrl,
        reconnectAttempts: parseInt(process.env.REACT_APP_WS_RECONNECT_ATTEMPTS || '5', 10),
        reconnectDelay: parseInt(process.env.REACT_APP_WS_RECONNECT_DELAY || '1000', 10),
      },
      ui: {
        autoRefreshInterval: parseInt(process.env.REACT_APP_AUTO_REFRESH_INTERVAL || '30000', 10),
        debugMode: process.env.REACT_APP_DEBUG_MODE === 'true',
        enableConsoleLogs: process.env.REACT_APP_ENABLE_CONSOLE_LOGS === 'true',
      },
      features: {
        enableAnalytics: process.env.REACT_APP_ENABLE_ANALYTICS === 'true',
        enableErrorReporting: process.env.REACT_APP_ENABLE_ERROR_REPORTING === 'true',
        enableStreaming: process.env.REACT_APP_ENABLE_STREAMING !== 'false', // Default to true
      },
    };
  }

  private getDefaultApiKey(): string {
    // In development/test, use test key. In production, this should fail fast.
    if (process.env.NODE_ENV === 'development' || process.env.NODE_ENV === 'test') {
      return 'test-api-key';
    }
    throw new Error('API key is required in production. Please set REACT_APP_API_KEY environment variable.');
  }

  private validateConfig(): void {
    const { api } = this.config;
    
    if (!api.baseUrl) {
      throw new Error('API base URL is required');
    }
    
    if (!api.key) {
      throw new Error('API key is required');
    }

    if (process.env.NODE_ENV === 'production' && api.key === 'test-api-key') {
      throw new Error('Test API key cannot be used in production');
    }
  }

  public get(): AppConfig {
    return { ...this.config }; // Return a copy to prevent mutations
  }

  public getApiConfig() {
    return { ...this.config.api };
  }

  public getUIConfig() {
    return { ...this.config.ui };
  }

  public getWebSocketConfig() {
    return { ...this.config.websocket };
  }

  public getFeaturesConfig() {
    return { ...this.config.features };
  }

  public isProduction(): boolean {
    return process.env.NODE_ENV === 'production';
  }

  public isDevelopment(): boolean {
    return process.env.NODE_ENV === 'development';
  }
}

// Singleton instance
export const configService = new ConfigService();

// Legacy CONFIG export for backward compatibility
export const CONFIG = {
  API_BASE_URL: configService.getApiConfig().baseUrl,
  WS_BASE_URL: configService.getWebSocketConfig().baseUrl,
  API_KEY: configService.getApiConfig().key,
  FEATURES: configService.getFeaturesConfig()
};

export default configService;