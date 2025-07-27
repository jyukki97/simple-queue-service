/**
 * Centralized Logging Utility
 * Provides conditional logging based on environment and configuration
 */

import configService from './config';

enum LogLevel {
  DEBUG = 0,
  INFO = 1,
  WARN = 2,
  ERROR = 3,
}

interface LogContext {
  component?: string;
  action?: string;
  userId?: string;
  sessionId?: string;
  [key: string]: any;
}

class Logger {
  private readonly isDevelopment: boolean;
  private readonly enableConsoleLogs: boolean;
  private readonly minLogLevel: LogLevel;

  constructor() {
    this.isDevelopment = configService.isDevelopment();
    this.enableConsoleLogs = configService.getUIConfig().enableConsoleLogs;
    this.minLogLevel = this.isDevelopment ? LogLevel.DEBUG : LogLevel.ERROR;
  }

  private shouldLog(level: LogLevel): boolean {
    return level >= this.minLogLevel && (this.enableConsoleLogs || level >= LogLevel.ERROR);
  }

  private formatMessage(level: string, message: string, context?: LogContext): string {
    const timestamp = new Date().toISOString();
    const contextStr = context ? ` | ${JSON.stringify(context)}` : '';
    return `[${timestamp}] ${level}: ${message}${contextStr}`;
  }

  debug(message: string, context?: LogContext): void {
    if (this.shouldLog(LogLevel.DEBUG)) {
      console.debug(this.formatMessage('DEBUG', message, context));
    }
  }

  info(message: string, context?: LogContext): void {
    if (this.shouldLog(LogLevel.INFO)) {
      console.info(this.formatMessage('INFO', message, context));
    }
  }

  warn(message: string, context?: LogContext): void {
    if (this.shouldLog(LogLevel.WARN)) {
      console.warn(this.formatMessage('WARN', message, context));
    }
  }

  error(message: string, error?: Error | any, context?: LogContext): void {
    if (this.shouldLog(LogLevel.ERROR)) {
      const errorDetails = error instanceof Error 
        ? { name: error.name, message: error.message, stack: error.stack }
        : error;
      
      const fullContext = { ...context, error: errorDetails };
      console.error(this.formatMessage('ERROR', message, fullContext));
    }
  }

  // Specialized logging methods for common use cases
  apiRequest(method: string, url: string, context?: LogContext): void {
    this.debug(`API Request: ${method.toUpperCase()} ${url}`, context);
  }

  apiResponse(status: number, url: string, duration?: number, context?: LogContext): void {
    const message = `API Response: ${status} ${url}${duration ? ` (${duration}ms)` : ''}`;
    if (status >= 400) {
      this.warn(message, context);
    } else {
      this.debug(message, context);
    }
  }

  componentMount(componentName: string, context?: LogContext): void {
    this.debug(`Component mounted: ${componentName}`, context);
  }

  componentUnmount(componentName: string, context?: LogContext): void {
    this.debug(`Component unmounted: ${componentName}`, context);
  }

  userAction(action: string, context?: LogContext): void {
    this.info(`User action: ${action}`, context);
  }

  performanceMetric(metric: string, value: number, unit: string = 'ms', context?: LogContext): void {
    this.debug(`Performance: ${metric} = ${value}${unit}`, context);
  }
}

// Singleton instance
export const logger = new Logger();
export default logger;