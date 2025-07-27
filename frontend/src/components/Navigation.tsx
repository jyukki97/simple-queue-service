import React, { memo } from 'react';
import { Link, useLocation } from 'react-router-dom';
import './Navigation.css';

const Navigation: React.FC = memo(() => {
  const location = useLocation();

  const isActive = (path: string) => {
    return location.pathname === path;
  };

  return (
    <nav className="navigation">
      <div className="nav-header">
        <h1>🎯 SQS Admin</h1>
        <p>Simple Queue Service 관리자 콘솔</p>
      </div>
      
      <ul className="nav-menu">
        <li>
          <Link 
            to="/dashboard" 
            className={`nav-link ${isActive('/dashboard') ? 'active' : ''}`}
          >
            📊 대시보드
          </Link>
        </li>
        <li>
          <Link 
            to="/queues" 
            className={`nav-link ${isActive('/queues') ? 'active' : ''}`}
          >
            📦 큐 관리
          </Link>
        </li>
        <li>
          <Link 
            to="/tenants" 
            className={`nav-link ${isActive('/tenants') ? 'active' : ''}`}
          >
            👥 테넌트 관리
          </Link>
        </li>
        <li>
          <Link 
            to="/monitoring" 
            className={`nav-link ${isActive('/monitoring') ? 'active' : ''}`}
          >
            📈 시스템 모니터링
          </Link>
        </li>
      </ul>

      <div className="nav-footer">
        <div className="nav-status">
          <span className="status-indicator healthy"></span>
          시스템 정상
        </div>
        <p className="nav-version">v1.0.0</p>
      </div>
    </nav>
  );
});

Navigation.displayName = 'Navigation';

export default Navigation;
