import React, { memo } from 'react';

interface StatCardProps {
  title: string;
  value: string | number;
  icon?: string;
  trend?: {
    value: number;
    isPositive: boolean;
  };
  className?: string;
  onClick?: () => void;
}

const StatCard: React.FC<StatCardProps> = memo(({
  title,
  value,
  icon,
  trend,
  className = '',
  onClick
}) => {
  const formatValue = (val: string | number): string => {
    if (typeof val === 'number') {
      return val.toLocaleString();
    }
    return val;
  };

  return (
    <div 
      className={`stat-card ${className} ${onClick ? 'clickable' : ''}`}
      onClick={onClick}
      role={onClick ? 'button' : 'article'}
      tabIndex={onClick ? 0 : undefined}
    >
      <div className="stat-card-header">
        {icon && <span className="stat-icon">{icon}</span>}
        <h3 className="stat-title">{title}</h3>
      </div>
      
      <div className="stat-value">{formatValue(value)}</div>
      
      {trend && (
        <div className={`stat-trend ${trend.isPositive ? 'positive' : 'negative'}`}>
          <span className="trend-icon">
            {trend.isPositive ? '↗️' : '↘️'}
          </span>
          <span className="trend-value">
            {Math.abs(trend.value)}%
          </span>
        </div>
      )}
    </div>
  );
});

StatCard.displayName = 'StatCard';

export default StatCard;