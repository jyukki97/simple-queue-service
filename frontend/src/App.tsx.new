import React from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import Dashboard from './components/Dashboard';
import QueueManagement from './components/QueueManagement';
import TenantManagement from './components/TenantManagement';
import SystemMonitoring from './components/SystemMonitoring';
import Navigation from './components/Navigation';
import './App.css';

function App() {
  return (
    <Router>
      <div className="App">
        <Navigation />
        <div className="App-content">
          <Routes>
            <Route path="/" element={<Navigate to="/dashboard" replace />} />
            <Route path="/dashboard" element={<Dashboard />} />
            <Route path="/queues" element={<QueueManagement />} />
            <Route path="/tenants" element={<TenantManagement />} />
            <Route path="/monitoring" element={<SystemMonitoring />} />
          </Routes>
        </div>
      </div>
    </Router>
  );
}

export default App;
