import React, { Suspense, useEffect } from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import {
  LazyDashboard,
  LazyQueueManagement,
  LazyTenantManagement,
  LazySystemMonitoring,
  preloadCriticalComponents
} from './components/LazyComponents';
import Navigation from './components/Navigation';
import ErrorBoundary from './components/ErrorBoundary';
import { initWebVitals } from './utils/performance';
import logger from './utils/logger';
import './App.css';

// Loading fallback component
const PageLoadingFallback: React.FC = () => (
  <div className="page-loading">
    <div className="loading-spinner"></div>
    <p>Loading page...</p>
  </div>
);

function App() {
  useEffect(() => {
    // Initialize Web Vitals monitoring
    initWebVitals((metric) => {
      logger.performanceMetric(metric.name, metric.value, 'ms', {
        component: 'App',
        metricId: metric.id,
        delta: metric.delta
      });
    });

    // Preload critical components after initial render
    preloadCriticalComponents();

    // Component mount logging
    logger.componentMount('App');

    return () => {
      logger.componentUnmount('App');
    };
  }, []);

  return (
    <ErrorBoundary>
      <Router>
        <div className="App">
          <Navigation />
          <div className="App-content">
            <Suspense fallback={<PageLoadingFallback />}>
              <Routes>
                <Route path="/" element={<Navigate to="/dashboard" replace />} />
                <Route 
                  path="/dashboard" 
                  element={
                    <ErrorBoundary>
                      <LazyDashboard />
                    </ErrorBoundary>
                  } 
                />
                <Route 
                  path="/queues" 
                  element={
                    <ErrorBoundary>
                      <LazyQueueManagement />
                    </ErrorBoundary>
                  } 
                />
                <Route 
                  path="/tenants" 
                  element={
                    <ErrorBoundary>
                      <LazyTenantManagement />
                    </ErrorBoundary>
                  } 
                />
                <Route 
                  path="/monitoring" 
                  element={
                    <ErrorBoundary>
                      <LazySystemMonitoring />
                    </ErrorBoundary>
                  } 
                />
              </Routes>
            </Suspense>
          </div>
        </div>
      </Router>
    </ErrorBoundary>
  );
}

export default App;
