import React, { useState, useEffect } from 'react';
import {
  MessageSearchRequest,
  MessageSearchResponse,
  FilterCriteria,
  FilterType,
  FilterOperator,
  SortField,
  SortOrder,
  StorageTier,
  EnhancedMessageResponse,
} from '../types/sqs';
import queueService from '../services/queueService';
import './AdvancedMessageSearch.css';

interface AdvancedMessageSearchProps {
  queueName: string;
}

const AdvancedMessageSearch: React.FC<AdvancedMessageSearchProps> = ({ queueName }) => {
  const [searchRequest, setSearchRequest] = useState<MessageSearchRequest>({
    pagination: { limit: 20, offset: 0 },
    sorting: { sortBy: SortField.SENT_TIME, sortOrder: SortOrder.DESC },
  });

  const [searchResults, setSearchResults] = useState<MessageSearchResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [filters, setFilters] = useState<FilterCriteria[]>([]);
  const [message, setMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null);

  // Filter builder state
  const [showFilterBuilder, setShowFilterBuilder] = useState(false);
  const [newFilter, setNewFilter] = useState<Partial<FilterCriteria>>({
    type: FilterType.ATTRIBUTE,
    operator: FilterOperator.EQUALS,
    enabled: true,
  });

  const filterTypeOptions = [
    { value: FilterType.ATTRIBUTE, label: 'Message Attribute' },
    { value: FilterType.CONTENT, label: 'Message Content' },
    { value: FilterType.TIME_RANGE, label: 'Time Range' },
    { value: FilterType.PRIORITY, label: 'Priority' },
    { value: FilterType.RECEIVE_COUNT, label: 'Receive Count' },
    { value: FilterType.TIER, label: 'Storage Tier' },
    { value: FilterType.MESSAGE_GROUP, label: 'Message Group' },
  ];

  const operatorOptions = {
    [FilterType.ATTRIBUTE]: [
      { value: FilterOperator.EQUALS, label: 'Equals' },
      { value: FilterOperator.NOT_EQUALS, label: 'Not Equals' },
      { value: FilterOperator.CONTAINS, label: 'Contains' },
      { value: FilterOperator.STARTS_WITH, label: 'Starts With' },
      { value: FilterOperator.REGEX, label: 'Regex Match' },
    ],
    [FilterType.CONTENT]: [
      { value: FilterOperator.CONTAINS, label: 'Contains' },
      { value: FilterOperator.NOT_CONTAINS, label: 'Not Contains' },
      { value: FilterOperator.REGEX, label: 'Regex Match' },
      { value: FilterOperator.STARTS_WITH, label: 'Starts With' },
    ],
    [FilterType.TIME_RANGE]: [
      { value: FilterOperator.BETWEEN, label: 'Between' },
      { value: FilterOperator.GREATER_THAN, label: 'After' },
      { value: FilterOperator.LESS_THAN, label: 'Before' },
    ],
    [FilterType.PRIORITY]: [
      { value: FilterOperator.EQUALS, label: 'Equals' },
      { value: FilterOperator.GREATER_THAN, label: 'Greater Than' },
      { value: FilterOperator.LESS_THAN, label: 'Less Than' },
      { value: FilterOperator.BETWEEN, label: 'Between' },
    ],
    [FilterType.RECEIVE_COUNT]: [
      { value: FilterOperator.EQUALS, label: 'Equals' },
      { value: FilterOperator.GREATER_THAN, label: 'Greater Than' },
      { value: FilterOperator.LESS_THAN, label: 'Less Than' },
      { value: FilterOperator.BETWEEN, label: 'Between' },
    ],
    [FilterType.TIER]: [
      { value: FilterOperator.EQUALS, label: 'Equals' },
      { value: FilterOperator.IN, label: 'In' },
    ],
    [FilterType.MESSAGE_GROUP]: [
      { value: FilterOperator.EQUALS, label: 'Equals' },
      { value: FilterOperator.CONTAINS, label: 'Contains' },
    ],
  };

  const executeSearch = async () => {
    setLoading(true);
    try {
      // Convert filters to search request format
      const convertedRequest = convertFiltersToSearchRequest();
      const response = await queueService.searchMessages(queueName, convertedRequest);
      
      setSearchResults(response);
      setMessage({
        type: 'success',
        text: `Found ${response.totalCount} messages in ${response.searchMetadata?.searchTimeMs || 0}ms`,
      });
    } catch (error) {
      setMessage({ type: 'error', text: 'Search request failed' });
    } finally {
      setLoading(false);
    }
  };

  const convertFiltersToSearchRequest = (): MessageSearchRequest => {
    const request: MessageSearchRequest = {
      ...searchRequest,
      attributeFilters: {},
    };

    filters.forEach((filter) => {
      if (!filter.enabled) return;

      switch (filter.type) {
        case FilterType.ATTRIBUTE:
          if (request.attributeFilters && filter.field && filter.value) {
            let value = String(filter.value);
            
            // Add operator prefixes for advanced matching
            switch (filter.operator) {
              case FilterOperator.NOT_EQUALS:
                value = `!${value}`;
                break;
              case FilterOperator.CONTAINS:
                value = `*${value}*`;
                break;
              case FilterOperator.STARTS_WITH:
                value = `${value}*`;
                break;
              case FilterOperator.REGEX:
                value = `regex:${value}`;
                break;
            }
            
            request.attributeFilters[filter.field] = value;
          }
          break;

        case FilterType.CONTENT:
          if (filter.value) {
            let contentFilter = String(filter.value);
            
            switch (filter.operator) {
              case FilterOperator.REGEX:
                contentFilter = `regex:${contentFilter}`;
                break;
              case FilterOperator.NOT_CONTAINS:
                contentFilter = `!${contentFilter}`;
                break;
              case FilterOperator.STARTS_WITH:
                contentFilter = `^${contentFilter}`;
                break;
            }
            
            request.contentFilter = contentFilter;
          }
          break;

        case FilterType.TIME_RANGE:
          // Handle time range logic here
          break;

        case FilterType.PRIORITY:
          if (filter.operator === FilterOperator.BETWEEN && Array.isArray(filter.value)) {
            request.priority = {
              minPriority: filter.value[0] as number,
              maxPriority: filter.value[1] as number,
            };
          } else if (typeof filter.value === 'number') {
            switch (filter.operator) {
              case FilterOperator.EQUALS:
                request.priority = { minPriority: filter.value, maxPriority: filter.value };
                break;
              case FilterOperator.GREATER_THAN:
                request.priority = { minPriority: filter.value + 1 };
                break;
              case FilterOperator.LESS_THAN:
                request.priority = { maxPriority: filter.value - 1 };
                break;
            }
          }
          break;

        case FilterType.TIER:
          if (filter.value) {
            request.tier = filter.value as StorageTier;
          }
          break;

        case FilterType.MESSAGE_GROUP:
          if (filter.value) {
            request.messageGroupId = String(filter.value);
          }
          break;
      }
    });

    return request;
  };

  const addFilter = () => {
    if (newFilter.type && newFilter.operator) {
      const filter: FilterCriteria = {
        id: Date.now().toString(),
        type: newFilter.type,
        field: newFilter.field || '',
        operator: newFilter.operator,
        value: newFilter.value || '',
        enabled: true,
      };
      
      setFilters([...filters, filter]);
      setNewFilter({
        type: FilterType.ATTRIBUTE,
        operator: FilterOperator.EQUALS,
        enabled: true,
      });
      setShowFilterBuilder(false);
    }
  };

  const removeFilter = (filterId: string) => {
    setFilters(filters.filter(f => f.id !== filterId));
  };

  const toggleFilter = (filterId: string) => {
    setFilters(filters.map(f => 
      f.id === filterId ? { ...f, enabled: !f.enabled } : f
    ));
  };

  const loadMore = async () => {
    if (!searchResults || !searchResults.hasMore) return;
    
    const nextRequest = {
      ...searchRequest,
      pagination: {
        ...searchRequest.pagination!,
        offset: (searchRequest.pagination?.offset || 0) + (searchRequest.pagination?.limit || 20),
      },
    };
    
    setSearchRequest(nextRequest);
    await executeSearch();
  };

  useEffect(() => {
    if (message) {
      const timer = setTimeout(() => setMessage(null), 5000);
      return () => clearTimeout(timer);
    }
  }, [message]);

  return (
    <div className="advanced-search">
      {message && (
        <div className={`message ${message.type}`}>
          {message.text}
        </div>
      )}

      {/* Filter Builder */}
      <div className="search-controls">
        <div className="filter-section">
          <div className="section-header">
            <h4>Search Filters</h4>
            <button
              onClick={() => setShowFilterBuilder(!showFilterBuilder)}
              className="btn btn-secondary btn-sm"
            >
              {showFilterBuilder ? 'Cancel' : 'Add Filter'}
            </button>
          </div>

          {showFilterBuilder && (
            <div className="filter-builder">
              <div className="form-row">
                <select
                  value={newFilter.type}
                  onChange={(e) => setNewFilter({ ...newFilter, type: e.target.value as FilterType })}
                  className="form-control"
                >
                  {filterTypeOptions.map(option => (
                    <option key={option.value} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </select>

                {newFilter.type && newFilter.type !== FilterType.TIME_RANGE && (
                  <input
                    type="text"
                    placeholder="Field name"
                    value={newFilter.field || ''}
                    onChange={(e) => setNewFilter({ ...newFilter, field: e.target.value })}
                    className="form-control"
                  />
                )}

                <select
                  value={newFilter.operator}
                  onChange={(e) => setNewFilter({ ...newFilter, operator: e.target.value as FilterOperator })}
                  className="form-control"
                >
                  {operatorOptions[newFilter.type as FilterType]?.map(option => (
                    <option key={option.value} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </select>

                <input
                  type="text"
                  placeholder="Value"
                  value={String(newFilter.value || '')}
                  onChange={(e) => setNewFilter({ ...newFilter, value: e.target.value })}
                  className="form-control"
                />

                <button onClick={addFilter} className="btn btn-primary btn-sm">
                  Add
                </button>
              </div>
            </div>
          )}

          {/* Active Filters */}
          <div className="active-filters">
            {filters.map((filter) => (
              <div key={filter.id} className={`filter-tag ${filter.enabled ? 'enabled' : 'disabled'}`}>
                <span className="filter-content">
                  {filter.type}: {filter.field} {filter.operator} {String(filter.value)}
                </span>
                <button onClick={() => toggleFilter(filter.id)} className="filter-toggle">
                  {filter.enabled ? '✓' : '✗'}
                </button>
                <button onClick={() => removeFilter(filter.id)} className="filter-remove">
                  ×
                </button>
              </div>
            ))}
          </div>
        </div>

        {/* Search Controls */}
        <div className="search-actions">
          <div className="form-row">
            <select
              value={searchRequest.sorting?.sortBy}
              onChange={(e) => setSearchRequest({
                ...searchRequest,
                sorting: { ...searchRequest.sorting!, sortBy: e.target.value as SortField }
              })}
              className="form-control"
            >
              <option value={SortField.SENT_TIME}>Sent Time</option>
              <option value={SortField.RECEIVE_COUNT}>Receive Count</option>
              <option value={SortField.PRIORITY}>Priority</option>
              <option value={SortField.MESSAGE_SIZE}>Message Size</option>
            </select>

            <select
              value={searchRequest.sorting?.sortOrder}
              onChange={(e) => setSearchRequest({
                ...searchRequest,
                sorting: { ...searchRequest.sorting!, sortOrder: e.target.value as SortOrder }
              })}
              className="form-control"
            >
              <option value={SortOrder.DESC}>Descending</option>
              <option value={SortOrder.ASC}>Ascending</option>
            </select>

            <input
              type="number"
              placeholder="Limit"
              value={searchRequest.pagination?.limit || 20}
              onChange={(e) => setSearchRequest({
                ...searchRequest,
                pagination: { ...searchRequest.pagination!, limit: parseInt(e.target.value) || 20 }
              })}
              className="form-control"
              min="1"
              max="100"
            />

            <button
              onClick={executeSearch}
              disabled={loading}
              className="btn btn-primary"
            >
              {loading ? 'Searching...' : 'Search'}
            </button>
          </div>
        </div>
      </div>

      {/* Search Results */}
      {searchResults && (
        <div className="search-results">
          <div className="results-header">
            <h4>
              Search Results ({searchResults.totalCount} total)
              {searchResults.searchMetadata && (
                <span className="search-meta">
                  - {searchResults.searchMetadata.searchTimeMs}ms
                  - Tiers: {searchResults.searchMetadata.tiersSearched.join(', ')}
                </span>
              )}
            </h4>
          </div>

          <div className="results-list">
            {searchResults.messages.map((message, index) => (
              <div key={message.messageId} className="message-result">
                <div className="message-header">
                  <div className="message-info">
                    <span className="message-id">ID: {message.messageId}</span>
                    {(message as EnhancedMessageResponse).currentTier && (
                      <span className={`tier-badge tier-${(message as EnhancedMessageResponse).currentTier?.toLowerCase()}`}>
                        {(message as EnhancedMessageResponse).currentTier}
                      </span>
                    )}
                  </div>
                  <div className="message-meta">
                    <span>Sent: {new Date(message.sentTimestamp).toLocaleString()}</span>
                  </div>
                </div>

                <div className="message-body">
                  {message.messageBody}
                </div>

                {Object.keys(message.attributes).length > 0 && (
                  <div className="message-attributes">
                    <strong>Attributes:</strong>
                    <pre>{JSON.stringify(message.attributes, null, 2)}</pre>
                  </div>
                )}
              </div>
            ))}
          </div>

          {searchResults.hasMore && (
            <div className="load-more">
              <button onClick={loadMore} className="btn btn-secondary">
                Load More Results
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default AdvancedMessageSearch;