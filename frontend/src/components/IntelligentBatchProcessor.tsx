import React, { useState, useEffect } from 'react';
import {
  BatchMessageRequest,
  BulkOperationRequest,
  BulkOperationType,
  BulkOperationResponse,
  StorageTier,
} from '../types/sqs';
import { sqsApi } from '../services/sqsApi';
import './IntelligentBatchProcessor.css';

interface IntelligentBatchProcessorProps {
  queueName: string;
}

interface BatchTemplate {
  id: string;
  name: string;
  messages: BatchMessageRequest[];
  settings: BatchSettings;
}

interface BatchSettings {
  optimizationMode: 'content' | 'priority' | 'time' | 'adaptive';
  batchSize: number;
  delayBetweenBatches: number;
  retryOnFailure: boolean;
  maxRetries: number;
}

const IntelligentBatchProcessor: React.FC<IntelligentBatchProcessorProps> = ({ queueName }) => {
  const [currentTab, setCurrentTab] = useState<'send' | 'bulk-ops'>('send');
  const [message, setMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null);
  const [loading, setLoading] = useState(false);

  // Batch sending state
  const [batchMessages, setBatchMessages] = useState<BatchMessageRequest[]>([
    { id: 'msg-1', messageBody: '', delaySeconds: 0 },
  ]);
  const [batchSettings, setBatchSettings] = useState<BatchSettings>({
    optimizationMode: 'adaptive',
    batchSize: 10,
    delayBetweenBatches: 100,
    retryOnFailure: true,
    maxRetries: 3,
  });
  const [templates, setTemplates] = useState<BatchTemplate[]>([]);

  // Bulk operations state
  const [bulkOperation, setBulkOperation] = useState<BulkOperationRequest>({
    operation: BulkOperationType.DELETE_BY_FILTER,
    filters: {},
  });
  const [lastBulkResult, setLastBulkResult] = useState<BulkOperationResponse | null>(null);

  // Batch message management
  const addBatchMessage = () => {
    const newId = `msg-${batchMessages.length + 1}`;
    setBatchMessages([
      ...batchMessages,
      { id: newId, messageBody: '', delaySeconds: 0 },
    ]);
  };

  const removeBatchMessage = (id: string) => {
    setBatchMessages(batchMessages.filter(msg => msg.id !== id));
  };

  const updateBatchMessage = (id: string, field: keyof BatchMessageRequest, value: any) => {
    setBatchMessages(batchMessages.map(msg =>
      msg.id === id ? { ...msg, [field]: value } : msg
    ));
  };

  const duplicateBatchMessage = (id: string) => {
    const original = batchMessages.find(msg => msg.id === id);
    if (original) {
      const newId = `msg-${Date.now()}`;
      const duplicate = { ...original, id: newId };
      setBatchMessages([...batchMessages, duplicate]);
    }
  };

  // Template management
  const saveAsTemplate = () => {
    const templateName = prompt('Enter template name:');
    if (templateName) {
      const template: BatchTemplate = {
        id: Date.now().toString(),
        name: templateName,
        messages: [...batchMessages],
        settings: { ...batchSettings },
      };
      setTemplates([...templates, template]);
      setMessage({ type: 'success', text: 'Template saved successfully' });
    }
  };

  const loadTemplate = (template: BatchTemplate) => {
    setBatchMessages([...template.messages]);
    setBatchSettings({ ...template.settings });
    setMessage({ type: 'success', text: `Template "${template.name}" loaded` });
  };

  const deleteTemplate = (templateId: string) => {
    setTemplates(templates.filter(t => t.id !== templateId));
    setMessage({ type: 'success', text: 'Template deleted' });
  };

  // Intelligent batch sending
  const sendIntelligentBatch = async () => {
    const validMessages = batchMessages.filter(msg => msg.messageBody.trim());
    if (validMessages.length === 0) {
      setMessage({ type: 'error', text: 'Please add at least one message' });
      return;
    }

    setLoading(true);
    try {
      // Simulate intelligent batching by splitting messages optimally
      const batches = optimizeBatches(validMessages);
      let totalSuccessful = 0;
      let totalFailed = 0;
      const allErrors: string[] = [];

      for (let i = 0; i < batches.length; i++) {
        const batch = batches[i];
        
        try {
          const response = await sqsApi.sendMessageBatch(queueName, { messages: batch });
          
          if (response.success && response.data) {
            totalSuccessful += response.data.successful.length;
            totalFailed += response.data.failed.length;
            response.data.failed.forEach(failure => 
              allErrors.push(`Batch ${i + 1}: ${failure.message}`)
            );
          }
          
          // Add delay between batches if configured
          if (i < batches.length - 1 && batchSettings.delayBetweenBatches > 0) {
            await new Promise(resolve => setTimeout(resolve, batchSettings.delayBetweenBatches));
          }
        } catch (error) {
          allErrors.push(`Batch ${i + 1}: Failed to send batch`);
          totalFailed += batch.length;
        }
      }

      setMessage({
        type: totalFailed === 0 ? 'success' : 'error',
        text: `Intelligent batch complete: ${totalSuccessful} successful, ${totalFailed} failed${
          allErrors.length > 0 ? `. Errors: ${allErrors.slice(0, 3).join('; ')}` : ''
        }`,
      });

    } catch (error) {
      setMessage({ type: 'error', text: 'Batch processing failed' });
    } finally {
      setLoading(false);
    }
  };

  const optimizeBatches = (messages: BatchMessageRequest[]): BatchMessageRequest[][] => {
    const batches: BatchMessageRequest[][] = [];
    const { batchSize, optimizationMode } = batchSettings;

    switch (optimizationMode) {
      case 'content':
        // Group by content similarity (simplified - by length ranges)
        const contentGroups = groupByContentSimilarity(messages);
        contentGroups.forEach(group => {
          for (let i = 0; i < group.length; i += batchSize) {
            batches.push(group.slice(i, i + batchSize));
          }
        });
        break;

      case 'priority':
        // Group by delay (priority simulation)
        const sortedByDelay = [...messages].sort((a, b) => (a.delaySeconds || 0) - (b.delaySeconds || 0));
        for (let i = 0; i < sortedByDelay.length; i += batchSize) {
          batches.push(sortedByDelay.slice(i, i + batchSize));
        }
        break;

      case 'time':
        // Simple time-based batching (chronological order)
        for (let i = 0; i < messages.length; i += batchSize) {
          batches.push(messages.slice(i, i + batchSize));
        }
        break;

      case 'adaptive':
      default:
        // Adaptive batching considering multiple factors
        const adaptiveBatches = createAdaptiveBatches(messages, batchSize);
        batches.push(...adaptiveBatches);
        break;
    }

    return batches.filter(batch => batch.length > 0);
  };

  const groupByContentSimilarity = (messages: BatchMessageRequest[]): BatchMessageRequest[][] => {
    const groups: { [key: string]: BatchMessageRequest[] } = {};
    
    messages.forEach(msg => {
      const lengthRange = Math.floor(msg.messageBody.length / 100) * 100;
      const key = `length-${lengthRange}`;
      if (!groups[key]) groups[key] = [];
      groups[key].push(msg);
    });

    return Object.values(groups);
  };

  const createAdaptiveBatches = (messages: BatchMessageRequest[], maxBatchSize: number): BatchMessageRequest[][] => {
    // Simplified adaptive algorithm - could be enhanced with ML
    const batches: BatchMessageRequest[][] = [];
    let currentBatch: BatchMessageRequest[] = [];
    let currentBatchSize = 0;
    const avgMessageSize = messages.reduce((sum, msg) => sum + msg.messageBody.length, 0) / messages.length;

    messages.forEach(msg => {
      const estimatedProcessingCost = msg.messageBody.length / avgMessageSize;
      
      if (currentBatch.length >= maxBatchSize || 
          (currentBatchSize + estimatedProcessingCost > maxBatchSize * 1.2)) {
        if (currentBatch.length > 0) {
          batches.push([...currentBatch]);
          currentBatch = [];
          currentBatchSize = 0;
        }
      }
      
      currentBatch.push(msg);
      currentBatchSize += estimatedProcessingCost;
    });

    if (currentBatch.length > 0) {
      batches.push(currentBatch);
    }

    return batches;
  };

  // Bulk operations
  const executeBulkOperation = async () => {
    setLoading(true);
    try {
      const response = await sqsApi.bulkOperation(queueName, bulkOperation);
      
      if (response.success && response.data) {
        setLastBulkResult(response.data);
        setMessage({
          type: response.data.failureCount === 0 ? 'success' : 'error',
          text: `Bulk operation completed: ${response.data.successCount} successful, ${response.data.failureCount} failed`,
        });
      } else {
        setMessage({ type: 'error', text: response.error?.message || 'Bulk operation failed' });
      }
    } catch (error) {
      setMessage({ type: 'error', text: 'Bulk operation request failed' });
    } finally {
      setLoading(false);
    }
  };

  const generateSampleMessages = (count: number) => {
    const samples: BatchMessageRequest[] = [];
    const templates = [
      'Order processing request for customer',
      'Notification email delivery',
      'Data synchronization task',
      'File processing job',
      'User activity log entry',
    ];

    for (let i = 0; i < count; i++) {
      const templateIndex = i % templates.length;
      samples.push({
        id: `sample-${i + 1}`,
        messageBody: `${templates[templateIndex]} #${i + 1} - Generated at ${new Date().toISOString()}`,
        delaySeconds: Math.floor(Math.random() * 300), // 0-5 minutes
        attributes: {
          priority: Math.floor(Math.random() * 10).toString(),
          category: `category-${templateIndex + 1}`,
          generated: 'true',
        },
      });
    }

    setBatchMessages(samples);
    setMessage({ type: 'success', text: `Generated ${count} sample messages` });
  };

  useEffect(() => {
    if (message) {
      const timer = setTimeout(() => setMessage(null), 5000);
      return () => clearTimeout(timer);
    }
  }, [message]);

  return (
    <div className="intelligent-batch-processor">
      {message && (
        <div className={`message ${message.type}`}>
          {message.text}
        </div>
      )}

      {/* Tab Navigation */}
      <div className="tab-navigation">
        <button
          className={`tab-button ${currentTab === 'send' ? 'active' : ''}`}
          onClick={() => setCurrentTab('send')}
        >
          Intelligent Batch Sending
        </button>
        <button
          className={`tab-button ${currentTab === 'bulk-ops' ? 'active' : ''}`}
          onClick={() => setCurrentTab('bulk-ops')}
        >
          Bulk Operations
        </button>
      </div>

      {currentTab === 'send' && (
        <div className="batch-sending-tab">
          {/* Batch Settings */}
          <div className="settings-section">
            <h4>Batch Optimization Settings</h4>
            <div className="settings-grid">
              <div className="setting-group">
                <label>Optimization Mode</label>
                <select
                  value={batchSettings.optimizationMode}
                  onChange={(e) => setBatchSettings({
                    ...batchSettings,
                    optimizationMode: e.target.value as any
                  })}
                  className="form-control"
                >
                  <option value="adaptive">Adaptive (Recommended)</option>
                  <option value="content">Content Similarity</option>
                  <option value="priority">Priority Based</option>
                  <option value="time">Time Based</option>
                </select>
              </div>

              <div className="setting-group">
                <label>Batch Size</label>
                <input
                  type="number"
                  value={batchSettings.batchSize}
                  onChange={(e) => setBatchSettings({
                    ...batchSettings,
                    batchSize: parseInt(e.target.value) || 10
                  })}
                  className="form-control"
                  min="1"
                  max="100"
                />
              </div>

              <div className="setting-group">
                <label>Delay Between Batches (ms)</label>
                <input
                  type="number"
                  value={batchSettings.delayBetweenBatches}
                  onChange={(e) => setBatchSettings({
                    ...batchSettings,
                    delayBetweenBatches: parseInt(e.target.value) || 0
                  })}
                  className="form-control"
                  min="0"
                />
              </div>

              <div className="setting-group">
                <label className="checkbox-label">
                  <input
                    type="checkbox"
                    checked={batchSettings.retryOnFailure}
                    onChange={(e) => setBatchSettings({
                      ...batchSettings,
                      retryOnFailure: e.target.checked
                    })}
                  />
                  Retry on Failure
                </label>
              </div>
            </div>
          </div>

          {/* Template Management */}
          <div className="template-section">
            <div className="section-header">
              <h4>Message Templates</h4>
              <div className="template-actions">
                <button onClick={saveAsTemplate} className="btn btn-secondary btn-sm">
                  Save as Template
                </button>
                <button onClick={() => generateSampleMessages(50)} className="btn btn-info btn-sm">
                  Generate 50 Samples
                </button>
                <button onClick={() => generateSampleMessages(100)} className="btn btn-info btn-sm">
                  Generate 100 Samples
                </button>
              </div>
            </div>

            {templates.length > 0 && (
              <div className="template-list">
                {templates.map(template => (
                  <div key={template.id} className="template-item">
                    <div className="template-info">
                      <strong>{template.name}</strong>
                      <span>{template.messages.length} messages</span>
                    </div>
                    <div className="template-actions">
                      <button
                        onClick={() => loadTemplate(template)}
                        className="btn btn-primary btn-sm"
                      >
                        Load
                      </button>
                      <button
                        onClick={() => deleteTemplate(template.id)}
                        className="btn btn-danger btn-sm"
                      >
                        Delete
                      </button>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Message Builder */}
          <div className="message-builder">
            <div className="section-header">
              <h4>Messages ({batchMessages.length})</h4>
              <div className="builder-actions">
                <button onClick={addBatchMessage} className="btn btn-secondary btn-sm">
                  Add Message
                </button>
                <button
                  onClick={sendIntelligentBatch}
                  disabled={loading}
                  className="btn btn-primary"
                >
                  {loading ? 'Processing...' : 'Send Intelligent Batch'}
                </button>
              </div>
            </div>

            <div className="message-list">
              {batchMessages.map((msg, index) => (
                <div key={msg.id} className="message-builder-item">
                  <div className="message-header">
                    <span className="message-number">#{index + 1}</span>
                    <div className="message-actions">
                      <button
                        onClick={() => duplicateBatchMessage(msg.id)}
                        className="btn btn-info btn-xs"
                        title="Duplicate"
                      >
                        📋
                      </button>
                      <button
                        onClick={() => removeBatchMessage(msg.id)}
                        className="btn btn-danger btn-xs"
                        title="Remove"
                      >
                        ✕
                      </button>
                    </div>
                  </div>

                  <div className="message-form">
                    <div className="form-group">
                      <label>Message Body</label>
                      <textarea
                        value={msg.messageBody}
                        onChange={(e) => updateBatchMessage(msg.id, 'messageBody', e.target.value)}
                        placeholder="Enter message content..."
                        className="form-control"
                        rows={3}
                      />
                    </div>

                    <div className="form-row">
                      <div className="form-group">
                        <label>Delay (seconds)</label>
                        <input
                          type="number"
                          value={msg.delaySeconds || 0}
                          onChange={(e) => updateBatchMessage(msg.id, 'delaySeconds', parseInt(e.target.value) || 0)}
                          className="form-control"
                          min="0"
                        />
                      </div>

                      <div className="form-group">
                        <label>Attributes (JSON)</label>
                        <input
                          type="text"
                          value={JSON.stringify(msg.attributes || {})}
                          onChange={(e) => {
                            try {
                              const attrs = JSON.parse(e.target.value);
                              updateBatchMessage(msg.id, 'attributes', attrs);
                            } catch {
                              // Ignore invalid JSON
                            }
                          }}
                          placeholder='{"key": "value"}'
                          className="form-control"
                        />
                      </div>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {currentTab === 'bulk-ops' && (
        <div className="bulk-operations-tab">
          <div className="bulk-operation-builder">
            <h4>Bulk Operations</h4>

            <div className="form-group">
              <label>Operation Type</label>
              <select
                value={bulkOperation.operation}
                onChange={(e) => setBulkOperation({
                  ...bulkOperation,
                  operation: e.target.value as BulkOperationType
                })}
                className="form-control"
              >
                <option value={BulkOperationType.DELETE_BY_FILTER}>Delete by Filter</option>
                <option value={BulkOperationType.DELETE_BY_IDS}>Delete by IDs</option>
                <option value={BulkOperationType.MOVE_BY_FILTER}>Move by Filter</option>
                <option value={BulkOperationType.PURGE_EXPIRED}>Purge Expired</option>
              </select>
            </div>

            {(bulkOperation.operation === BulkOperationType.DELETE_BY_IDS ||
              bulkOperation.operation === BulkOperationType.CHANGE_VISIBILITY_BY_IDS) && (
              <div className="form-group">
                <label>Message IDs (comma separated)</label>
                <textarea
                  value={bulkOperation.messageIds?.join(', ') || ''}
                  onChange={(e) => setBulkOperation({
                    ...bulkOperation,
                    messageIds: e.target.value.split(',').map(id => id.trim()).filter(id => id)
                  })}
                  placeholder="msg-1, msg-2, msg-3..."
                  className="form-control"
                  rows={3}
                />
              </div>
            )}

            {(bulkOperation.operation === BulkOperationType.DELETE_BY_FILTER ||
              bulkOperation.operation === BulkOperationType.MOVE_BY_FILTER) && (
              <div className="filter-builder">
                <h5>Filter Criteria</h5>
                
                <div className="form-group">
                  <label>Attribute Filters (JSON)</label>
                  <textarea
                    value={JSON.stringify(bulkOperation.filters?.attributeFilters || {}, null, 2)}
                    onChange={(e) => {
                      try {
                        const attrs = JSON.parse(e.target.value);
                        setBulkOperation({
                          ...bulkOperation,
                          filters: { ...bulkOperation.filters, attributeFilters: attrs }
                        });
                      } catch {
                        // Ignore invalid JSON
                      }
                    }}
                    placeholder='{"priority": "low", "category": "notifications"}'
                    className="form-control"
                    rows={4}
                  />
                </div>

                <div className="form-row">
                  <div className="form-group">
                    <label>Content Filter</label>
                    <input
                      type="text"
                      value={bulkOperation.filters?.contentFilter || ''}
                      onChange={(e) => setBulkOperation({
                        ...bulkOperation,
                        filters: { ...bulkOperation.filters, contentFilter: e.target.value }
                      })}
                      placeholder="Search term or regex:pattern"
                      className="form-control"
                    />
                  </div>

                  <div className="form-group">
                    <label>Storage Tier</label>
                    <select
                      value={bulkOperation.filters?.currentTier || ''}
                      onChange={(e) => setBulkOperation({
                        ...bulkOperation,
                        filters: { ...bulkOperation.filters, currentTier: e.target.value as StorageTier }
                      })}
                      className="form-control"
                    >
                      <option value="">Any Tier</option>
                      <option value={StorageTier.HOT}>Hot</option>
                      <option value={StorageTier.WARM}>Warm</option>
                      <option value={StorageTier.COLD}>Cold</option>
                    </select>
                  </div>
                </div>
              </div>
            )}

            <div className="bulk-actions">
              <button
                onClick={executeBulkOperation}
                disabled={loading}
                className="btn btn-warning"
              >
                {loading ? 'Executing...' : 'Execute Bulk Operation'}
              </button>
            </div>
          </div>

          {lastBulkResult && (
            <div className="bulk-result">
              <h5>Last Operation Result</h5>
              <div className="result-summary">
                <div className="result-stat">
                  <span className="stat-label">Operation:</span>
                  <span className="stat-value">{lastBulkResult.operation}</span>
                </div>
                <div className="result-stat">
                  <span className="stat-label">Total Requested:</span>
                  <span className="stat-value">{lastBulkResult.totalRequested}</span>
                </div>
                <div className="result-stat">
                  <span className="stat-label">Successful:</span>
                  <span className="stat-value success">{lastBulkResult.successCount}</span>
                </div>
                <div className="result-stat">
                  <span className="stat-label">Failed:</span>
                  <span className="stat-value error">{lastBulkResult.failureCount}</span>
                </div>
                <div className="result-stat">
                  <span className="stat-label">Processing Time:</span>
                  <span className="stat-value">{lastBulkResult.processingTimeMs}ms</span>
                </div>
              </div>

              {lastBulkResult.errors.length > 0 && (
                <div className="error-details">
                  <h6>Errors:</h6>
                  <ul>
                    {lastBulkResult.errors.slice(0, 10).map((error, index) => (
                      <li key={index}>
                        {error.messageId ? `${error.messageId}: ` : ''}
                        {error.errorMessage}
                      </li>
                    ))}
                  </ul>
                  {lastBulkResult.errors.length > 10 && (
                    <p>... and {lastBulkResult.errors.length - 10} more errors</p>
                  )}
                </div>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default IntelligentBatchProcessor;