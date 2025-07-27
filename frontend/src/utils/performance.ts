/**
 * Performance Optimization Utilities
 * Provides tools for monitoring and optimizing application performance
 */

import { ReportHandler } from 'web-vitals';

// Performance monitoring
export const performance = {
  /**
   * Measure and log function execution time
   */
  measureFunction: <T extends any[], R>(
    fn: (...args: T) => R,
    label: string
  ) => {
    return (...args: T): R => {
      const startTime = Date.now();
      const result = fn(...args);
      const endTime = Date.now();
      
      if (process.env.NODE_ENV === 'development') {
        console.debug(`⏱️ ${label}: ${endTime - startTime}ms`);
      }
      
      return result;
    };
  },

  /**
   * Measure async function execution time
   */
  measureAsyncFunction: <T extends any[], R>(
    fn: (...args: T) => Promise<R>,
    label: string
  ) => {
    return async (...args: T): Promise<R> => {
      const startTime = Date.now();
      const result = await fn(...args);
      const endTime = Date.now();
      
      if (process.env.NODE_ENV === 'development') {
        console.debug(`⏱️ ${label}: ${endTime - startTime}ms`);
      }
      
      return result;
    };
  },

  /**
   * Create a performance mark
   */
  mark: (name: string): void => {
    if ('performance' in window && 'mark' in window.performance) {
      window.performance.mark(name);
    }
  },

  /**
   * Measure between two performance marks
   */
  measure: (name: string, startMark: string, endMark?: string): number => {
    if ('performance' in window && 'measure' in window.performance) {
      try {
        const measure = window.performance.measure(name, startMark, endMark);
        return measure.duration;
      } catch (error) {
        console.warn('Performance measurement failed:', error);
        return 0;
      }
    }
    return 0;
  },
};

// Debounce utility for performance optimization
export const debounce = <T extends any[]>(
  func: (...args: T) => void,
  wait: number
): ((...args: T) => void) => {
  let timeout: NodeJS.Timeout;

  return function executedFunction(...args: T) {
    const later = () => {
      clearTimeout(timeout);
      func(...args);
    };

    clearTimeout(timeout);
    timeout = setTimeout(later, wait);
  };
};

// Throttle utility for performance optimization
export const throttle = <T extends any[]>(
  func: (...args: T) => void,
  limit: number
): ((...args: T) => void) => {
  let inThrottle: boolean;

  return function executedFunction(...args: T) {
    if (!inThrottle) {
      func(...args);
      inThrottle = true;
      setTimeout(() => inThrottle = false, limit);
    }
  };
};

// Lazy loading utilities
export const lazy = {
  /**
   * Create intersection observer for lazy loading
   */
  createObserver: (
    callback: (entries: IntersectionObserverEntry[]) => void,
    options: IntersectionObserverInit = {}
  ): IntersectionObserver | null => {
    if ('IntersectionObserver' in window) {
      return new IntersectionObserver(callback, {
        rootMargin: '50px',
        threshold: 0.1,
        ...options,
      });
    }
    return null;
  },

  /**
   * Lazy load images
   */
  loadImage: (src: string): Promise<HTMLImageElement> => {
    return new Promise((resolve, reject) => {
      const img = new Image();
      img.onload = () => resolve(img);
      img.onerror = reject;
      img.src = src;
    });
  },
};

// Memory optimization utilities
export const memory = {
  /**
   * Clean up event listeners and timers
   */
  cleanup: (cleanupCallbacks: Array<() => void>): void => {
    cleanupCallbacks.forEach(callback => {
      try {
        callback();
      } catch (error) {
        console.warn('Cleanup callback failed:', error);
      }
    });
  },

  /**
   * Monitor memory usage (development only)
   */
  monitor: (): void => {
    if (process.env.NODE_ENV === 'development' && 'memory' in performance) {
      const memInfo = (performance as any).memory;
      console.debug('🧠 Memory Usage:', {
        used: `${Math.round(memInfo.usedJSHeapSize / 1048576)}MB`,
        total: `${Math.round(memInfo.totalJSHeapSize / 1048576)}MB`,
        limit: `${Math.round(memInfo.jsHeapSizeLimit / 1048576)}MB`,
      });
    }
  },
};

// Bundle size optimization recommendations
export const bundleOptimization = {
  /**
   * Analyze and suggest optimizations
   */
  suggestions: {
    // Use React.lazy for route-based code splitting
    routeSplitting: `
      // Example: Route-based code splitting
      const Dashboard = React.lazy(() => import('./components/Dashboard'));
      const QueueManagement = React.lazy(() => import('./components/QueueManagement'));
    `,

    // Use dynamic imports for heavy libraries
    dynamicImports: `
      // Example: Dynamic import for heavy libraries
      const loadCharts = async () => {
        const { LineChart } = await import('recharts');
        return LineChart;
      };
    `,

    // Tree shake unused code
    treeShaking: `
      // Example: Import only needed functions
      import { format } from 'date-fns/format';
      // Instead of: import * as dateFns from 'date-fns';
    `,
  },
};

// Web Vitals monitoring
export const initWebVitals = (onPerfEntry?: ReportHandler): void => {
  if (onPerfEntry && onPerfEntry instanceof Function) {
    import('web-vitals').then(({ getCLS, getFID, getFCP, getLCP, getTTFB }) => {
      getCLS(onPerfEntry);
      getFID(onPerfEntry);
      getFCP(onPerfEntry);
      getLCP(onPerfEntry);
      getTTFB(onPerfEntry);
    });
  }
};

// Performance budget checker
export const performanceBudget = {
  limits: {
    bundleSize: 2 * 1024 * 1024, // 2MB
    firstContentfulPaint: 2000, // 2s
    largestContentfulPaint: 2500, // 2.5s
    firstInputDelay: 100, // 100ms
    cumulativeLayoutShift: 0.1,
  },

  check: (metric: string, value: number): boolean => {
    const limit = (performanceBudget.limits as any)[metric];
    if (limit && value > limit) {
      console.warn(`⚠️ Performance budget exceeded for ${metric}: ${value} > ${limit}`);
      return false;
    }
    return true;
  },
};

const PerformanceUtils = {
  performance,
  debounce,
  throttle,
  lazy,
  memory,
  bundleOptimization,
  initWebVitals,
  performanceBudget,
};

export default PerformanceUtils;