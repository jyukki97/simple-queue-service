import React, { memo, forwardRef } from 'react';
import './ValidatedInput.css';

interface ValidatedInputProps extends Omit<React.InputHTMLAttributes<HTMLInputElement>, 'onChange'> {
  label?: string;
  errors?: string[];
  touched?: boolean;
  helpText?: string;
  required?: boolean;
  onChange: (value: string) => void;
  onBlur?: () => void;
}

export const ValidatedInput = memo(forwardRef<HTMLInputElement, ValidatedInputProps>(({
  label,
  errors = [],
  touched = false,
  helpText,
  required = false,
  onChange,
  onBlur,
  className = '',
  id,
  ...inputProps
}, ref) => {
  const hasErrors = touched && errors.length > 0;
  const inputId = id || `input-${Math.random().toString(36).substr(2, 9)}`;

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    onChange(e.target.value);
  };

  const handleBlur = () => {
    if (onBlur) {
      onBlur();
    }
  };

  return (
    <div className={`validated-input ${hasErrors ? 'has-error' : ''} ${className}`}>
      {label && (
        <label htmlFor={inputId} className="input-label">
          {label}
          {required && <span className="required-indicator" aria-label="required">*</span>}
        </label>
      )}
      
      <div className="input-wrapper">
        <input
          ref={ref}
          id={inputId}
          className={`input-field ${hasErrors ? 'error' : ''}`}
          onChange={handleChange}
          onBlur={handleBlur}
          aria-invalid={hasErrors}
          aria-describedby={
            hasErrors ? `${inputId}-errors` : 
            helpText ? `${inputId}-help` : undefined
          }
          {...inputProps}
        />
        
        {hasErrors && (
          <div className="error-icon" aria-hidden="true">
            ⚠️
          </div>
        )}
      </div>

      {helpText && !hasErrors && (
        <div id={`${inputId}-help`} className="help-text">
          {helpText}
        </div>
      )}

      {hasErrors && (
        <div id={`${inputId}-errors`} className="error-messages" role="alert">
          {errors.map((error, index) => (
            <div key={index} className="error-message">
              {error}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}));

ValidatedInput.displayName = 'ValidatedInput';

export default ValidatedInput;