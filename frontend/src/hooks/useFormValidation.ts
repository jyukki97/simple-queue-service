import { useState, useCallback, useMemo } from 'react';
import { ValidationResult } from '../utils/validation';

interface FormField<T = any> {
  value: T;
  errors: string[];
  touched: boolean;
  isValid: boolean;
}

interface ValidationRule<T = any> {
  validator: (value: T) => ValidationResult;
  debounceMs?: number;
}

interface FormValidationConfig<T extends Record<string, any>> {
  initialValues: T;
  validationRules: Partial<Record<keyof T, ValidationRule[]>>;
  validateOnChange?: boolean;
  validateOnBlur?: boolean;
}

export const useFormValidation = <T extends Record<string, any>>(
  config: FormValidationConfig<T>
) => {
  const [fields, setFields] = useState<Record<keyof T, FormField>>(() => {
    const initialFields: Record<keyof T, FormField> = {} as Record<keyof T, FormField>;
    
    Object.keys(config.initialValues).forEach(key => {
      const fieldKey = key as keyof T;
      initialFields[fieldKey] = {
        value: config.initialValues[fieldKey],
        errors: [],
        touched: false,
        isValid: true,
      };
    });
    
    return initialFields;
  });

  const [isSubmitting, setIsSubmitting] = useState(false);

  // Validate a single field
  const validateField = useCallback((
    fieldName: keyof T, 
    value: T[keyof T], 
    shouldTouch: boolean = false
  ): FormField => {
    const rules = config.validationRules[fieldName] || [];
    const allErrors: string[] = [];
    let sanitizedValue = value;

    // Run all validation rules for this field
    for (const rule of rules) {
      const result = rule.validator(value);
      if (!result.isValid) {
        allErrors.push(...result.errors);
      }
      if (result.sanitizedValue !== undefined) {
        sanitizedValue = result.sanitizedValue;
      }
    }

    return {
      value: sanitizedValue,
      errors: allErrors,
      touched: shouldTouch || fields[fieldName]?.touched || false,
      isValid: allErrors.length === 0,
    };
  }, [config.validationRules, fields]);

  // Update a single field value
  const setFieldValue = useCallback((fieldName: keyof T, value: T[keyof T]) => {
    const shouldValidate = config.validateOnChange ?? true;
    const validatedField = shouldValidate 
      ? validateField(fieldName, value, false)
      : { ...fields[fieldName], value };

    setFields(prev => ({
      ...prev,
      [fieldName]: validatedField,
    }));
  }, [config.validateOnChange, validateField, fields]);

  // Mark field as touched (usually on blur)
  const setFieldTouched = useCallback((fieldName: keyof T, touched: boolean = true) => {
    const shouldValidate = (config.validateOnBlur ?? true) && touched;
    const currentField = fields[fieldName];
    
    if (!currentField) return;

    const validatedField = shouldValidate
      ? validateField(fieldName, currentField.value, touched)
      : { ...currentField, touched };

    setFields(prev => ({
      ...prev,
      [fieldName]: validatedField,
    }));
  }, [config.validateOnBlur, fields, validateField]);

  // Validate all fields
  const validateAllFields = useCallback((): boolean => {
    const validatedFields: Record<keyof T, FormField> = {} as Record<keyof T, FormField>;
    let hasErrors = false;

    Object.keys(fields).forEach(key => {
      const fieldKey = key as keyof T;
      const currentField = fields[fieldKey];
      const validatedField = validateField(fieldKey, currentField.value, true);
      
      validatedFields[fieldKey] = validatedField;
      
      if (!validatedField.isValid) {
        hasErrors = true;
      }
    });

    setFields(validatedFields);
    return !hasErrors;
  }, [fields, validateField]);

  // Reset form to initial values
  const resetForm = useCallback(() => {
    const resetFields: Record<keyof T, FormField> = {} as Record<keyof T, FormField>;
    
    Object.keys(config.initialValues).forEach(key => {
      const fieldKey = key as keyof T;
      resetFields[fieldKey] = {
        value: config.initialValues[fieldKey],
        errors: [],
        touched: false,
        isValid: true,
      };
    });
    
    setFields(resetFields);
    setIsSubmitting(false);
  }, [config.initialValues]);

  // Get current form values
  const values = useMemo(() => {
    const currentValues: Partial<T> = {};
    Object.keys(fields).forEach(key => {
      const fieldKey = key as keyof T;
      currentValues[fieldKey] = fields[fieldKey].value;
    });
    return currentValues as T;
  }, [fields]);

  // Check if form is valid
  const isValid = useMemo(() => {
    return Object.values(fields).every(field => field.isValid);
  }, [fields]);

  // Check if any field has been touched
  const isTouched = useMemo(() => {
    return Object.values(fields).some(field => field.touched);
  }, [fields]);

  // Get all errors
  const errors = useMemo(() => {
    const allErrors: Record<keyof T, string[]> = {} as Record<keyof T, string[]>;
    Object.keys(fields).forEach(key => {
      const fieldKey = key as keyof T;
      const field = fields[fieldKey];
      if (field.errors.length > 0) {
        allErrors[fieldKey] = field.errors;
      }
    });
    return allErrors;
  }, [fields]);

  // Handle form submission
  const handleSubmit = useCallback(async (
    onSubmit: (values: T) => Promise<void> | void
  ) => {
    setIsSubmitting(true);
    
    try {
      const isFormValid = validateAllFields();
      
      if (isFormValid) {
        await onSubmit(values);
      }
    } finally {
      setIsSubmitting(false);
    }
  }, [validateAllFields, values]);

  return {
    fields,
    values,
    errors,
    isValid,
    isTouched,
    isSubmitting,
    setFieldValue,
    setFieldTouched,
    validateField,
    validateAllFields,
    resetForm,
    handleSubmit,
  };
};