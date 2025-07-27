/**
 * Input Validation and Sanitization Utilities
 * Provides comprehensive validation and sanitization for user inputs
 */

// Validation result interface
export interface ValidationResult {
  isValid: boolean;
  errors: string[];
  sanitizedValue?: any;
}

// Common validation patterns
export const ValidationPatterns = {
  EMAIL: /^[^\s@]+@[^\s@]+\.[^\s@]+$/,
  QUEUE_NAME: /^[a-zA-Z0-9_-]{1,80}$/,
  TENANT_ID: /^[a-zA-Z0-9_-]{1,64}$/,
  MESSAGE_ID: /^[a-zA-Z0-9_-]{1,128}$/,
  API_KEY: /^[a-zA-Z0-9_-]{16,128}$/,
  URL: /^https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)$/,
  ALPHANUMERIC: /^[a-zA-Z0-9]+$/,
  NUMERIC: /^\d+$/,
  FLOAT: /^-?\d*\.?\d+$/,
} as const;

// Sanitization functions
export const sanitize = {
  /**
   * Remove HTML tags and encode special characters
   */
  html: (input: string): string => {
    return input
      .replace(/<[^>]*>/g, '') // Remove HTML tags
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#x27;')
      .replace(/\//g, '&#x2F;');
  },

  /**
   * Remove potentially dangerous characters for SQL/NoSQL injection
   */
  database: (input: string): string => {
    return input
      .replace(/['"`;\\]/g, '') // Remove quotes, semicolons, backslashes
      .replace(/--/g, '') // Remove comment markers
      .replace(/\/\*/g, '') // Remove comment start
      .replace(/\*\//g, '') // Remove comment end
      .trim();
  },

  /**
   * Sanitize JSON string input
   */
  json: (input: string): string => {
    try {
      // Parse and stringify to validate and normalize
      const parsed = JSON.parse(input);
      return JSON.stringify(parsed);
    } catch {
      return '{}'; // Return empty object if invalid JSON
    }
  },

  /**
   * Remove whitespace and normalize string
   */
  string: (input: string): string => {
    return input.trim().replace(/\s+/g, ' ');
  },

  /**
   * Sanitize number input
   */
  number: (input: string | number): number => {
    const num = typeof input === 'string' ? parseFloat(input) : input;
    return isNaN(num) ? 0 : num;
  },

  /**
   * Sanitize integer input
   */
  integer: (input: string | number): number => {
    const num = typeof input === 'string' ? parseInt(input, 10) : Math.floor(input);
    return isNaN(num) ? 0 : num;
  },

  /**
   * Sanitize URL input
   */
  url: (input: string): string => {
    try {
      const url = new URL(input);
      return url.toString();
    } catch {
      return '';
    }
  },
};

// Validation functions
export const validate = {
  /**
   * Validate required field
   */
  required: (value: any, fieldName: string = 'Field'): ValidationResult => {
    const isEmpty = value === null || value === undefined || 
                   (typeof value === 'string' && value.trim() === '') ||
                   (Array.isArray(value) && value.length === 0);

    return {
      isValid: !isEmpty,
      errors: isEmpty ? [`${fieldName} is required`] : [],
      sanitizedValue: value,
    };
  },

  /**
   * Validate string length
   */
  stringLength: (
    value: string, 
    min: number = 0, 
    max: number = Infinity, 
    fieldName: string = 'Field'
  ): ValidationResult => {
    const sanitized = sanitize.string(value);
    const length = sanitized.length;
    const errors: string[] = [];

    if (length < min) {
      errors.push(`${fieldName} must be at least ${min} characters long`);
    }
    if (length > max) {
      errors.push(`${fieldName} must be no more than ${max} characters long`);
    }

    return {
      isValid: errors.length === 0,
      errors,
      sanitizedValue: sanitized,
    };
  },

  /**
   * Validate against regex pattern
   */
  pattern: (
    value: string, 
    pattern: RegExp, 
    errorMessage: string,
    fieldName: string = 'Field'
  ): ValidationResult => {
    const sanitized = sanitize.string(value);
    const isValid = pattern.test(sanitized);

    return {
      isValid,
      errors: isValid ? [] : [`${fieldName}: ${errorMessage}`],
      sanitizedValue: sanitized,
    };
  },

  /**
   * Validate email format
   */
  email: (value: string, fieldName: string = 'Email'): ValidationResult => {
    return validate.pattern(
      value,
      ValidationPatterns.EMAIL,
      'must be a valid email address',
      fieldName
    );
  },

  /**
   * Validate queue name
   */
  queueName: (value: string, fieldName: string = 'Queue name'): ValidationResult => {
    const lengthResult = validate.stringLength(value, 1, 80, fieldName);
    if (!lengthResult.isValid) {
      return lengthResult;
    }

    return validate.pattern(
      value,
      ValidationPatterns.QUEUE_NAME,
      'can only contain letters, numbers, hyphens, and underscores',
      fieldName
    );
  },

  /**
   * Validate tenant ID
   */
  tenantId: (value: string, fieldName: string = 'Tenant ID'): ValidationResult => {
    const lengthResult = validate.stringLength(value, 1, 64, fieldName);
    if (!lengthResult.isValid) {
      return lengthResult;
    }

    return validate.pattern(
      value,
      ValidationPatterns.TENANT_ID,
      'can only contain letters, numbers, hyphens, and underscores',
      fieldName
    );
  },

  /**
   * Validate number range
   */
  numberRange: (
    value: number, 
    min: number = -Infinity, 
    max: number = Infinity,
    fieldName: string = 'Number'
  ): ValidationResult => {
    const sanitized = sanitize.number(value);
    const errors: string[] = [];

    if (sanitized < min) {
      errors.push(`${fieldName} must be at least ${min}`);
    }
    if (sanitized > max) {
      errors.push(`${fieldName} must be no more than ${max}`);
    }

    return {
      isValid: errors.length === 0,
      errors,
      sanitizedValue: sanitized,
    };
  },

  /**
   * Validate JSON string
   */
  json: (value: string, fieldName: string = 'JSON'): ValidationResult => {
    try {
      const sanitized = sanitize.json(value);
      JSON.parse(sanitized); // Validate it parses correctly
      return {
        isValid: true,
        errors: [],
        sanitizedValue: sanitized,
      };
    } catch (error) {
      return {
        isValid: false,
        errors: [`${fieldName} must be valid JSON`],
        sanitizedValue: '{}',
      };
    }
  },

  /**
   * Validate URL
   */
  url: (value: string, fieldName: string = 'URL'): ValidationResult => {
    const sanitized = sanitize.url(value);
    const isValid = sanitized !== '';

    return {
      isValid,
      errors: isValid ? [] : [`${fieldName} must be a valid URL`],
      sanitizedValue: sanitized,
    };
  },
};

/**
 * Validate multiple fields at once
 */
export const validateFields = (
  validations: Array<() => ValidationResult>
): ValidationResult => {
  const results = validations.map(validation => validation());
  const allErrors = results.flatMap(result => result.errors);
  const isValid = allErrors.length === 0;

  return {
    isValid,
    errors: allErrors,
    sanitizedValue: results.map(result => result.sanitizedValue),
  };
};

/**
 * Create a validator function for form fields
 */
export const createValidator = (
  validations: ValidationResult[]
): ValidationResult => {
  const allErrors = validations.flatMap(result => result.errors);
  const isValid = allErrors.length === 0;

  return {
    isValid,
    errors: allErrors,
  };
};