/**
 * Lazy Loading Components
 * Route-based code splitting for better performance
 */

import React, { Suspense, lazy } from 'react';
import ErrorBoundary from './ErrorBoundary';
import './LazyComponents.css';

// Loading spinner component
const LoadingSpinner: React.FC<{ message?: string }> = ({ message = 'Loading...' }) => (
  <div className="lazy-loading-container">
    <div className="loading-spinner">
      <div className="spinner-ring"></div>
    </div>
    <p className="loading-message">{message}</p>
  </div>
);

// Lazy load main components
export const LazyDashboard = lazy(() => 
  import('./Dashboard').then(module => ({
    default: module.default
  }))
);

export const LazyQueueManagement = lazy(() =>
  import('./QueueManagement').then(module => ({
    default: module.default
  }))
);

export const LazyTenantManagement = lazy(() =>
  import('./TenantManagement').then(module => ({
    default: module.default
  }))
);

export const LazySystemMonitoring = lazy(() =>
  import('./SystemMonitoring').then(module => ({
    default: module.default
  }))
);

// Heavy components that should be loaded on demand
export const LazyAdvancedMessageSearch = lazy(() =>
  import('./AdvancedMessageSearch').then(module => ({
    default: module.default
  }))
);

export const LazyIntelligentBatchProcessor = lazy(() =>
  import('./IntelligentBatchProcessor').then(module => ({
    default: module.default
  }))
);

export const LazyStorageTierManager = lazy(() =>
  import('./StorageTierManager').then(module => ({
    default: module.default
  }))
);

// Higher-order component for lazy loading with error boundary
export const withLazyLoading = <P extends object>(
  Component: React.ComponentType<P>,
  loadingMessage?: string,
  fallbackComponent?: React.ComponentType
) => {
  const LazyComponent = lazy(() => Promise.resolve({ default: Component }));
  
  return React.forwardRef<any, P>((props, ref) => {
    const FallbackComponent = fallbackComponent;
    return (
      <ErrorBoundary fallback={FallbackComponent ? <FallbackComponent /> : undefined}>
        <Suspense fallback={<LoadingSpinner message={loadingMessage} />}>
          <LazyComponent {...props} ref={ref} />
        </Suspense>
      </ErrorBoundary>
    );
  });
};

// Route wrapper with lazy loading
export const LazyRoute: React.FC<{
  component: React.ComponentType<any>;
  loadingMessage?: string;
  errorFallback?: React.ComponentType;
  [key: string]: any;
}> = ({ component: Component, loadingMessage, errorFallback, ...props }) => {
  const ErrorFallback = errorFallback;
  return (
    <ErrorBoundary fallback={ErrorFallback ? <ErrorFallback /> : undefined}>
      <Suspense fallback={<LoadingSpinner message={loadingMessage} />}>
        <Component {...props} />
      </Suspense>
    </ErrorBoundary>
  );
};

// Preload utility for critical components
export const preloadComponent = (componentImport: () => Promise<any>): void => {
  // Preload after a short delay to avoid blocking initial render
  setTimeout(() => {
    componentImport().catch(error => {
      console.warn('Failed to preload component:', error);
    });
  }, 1000);
};

// Preload critical components after initial load
export const preloadCriticalComponents = (): void => {
  preloadComponent(() => import('./Dashboard'));
  preloadComponent(() => import('./QueueManagement'));
};

const LazyComponentsExport = {
  LazyDashboard,
  LazyQueueManagement,
  LazyTenantManagement,
  LazySystemMonitoring,
  LazyAdvancedMessageSearch,
  LazyIntelligentBatchProcessor,
  LazyStorageTierManager,
  withLazyLoading,
  LazyRoute,
  preloadComponent,
  preloadCriticalComponents,
};

export default LazyComponentsExport;