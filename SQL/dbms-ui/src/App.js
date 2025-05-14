import React, { useState } from 'react';
import './App.css';

function App() {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState([]);
  const [history, setHistory] = useState([]);
  const [currentDb, setCurrentDb] = useState('No database selected');
  const [isLoading, setIsLoading] = useState(false);
  const [activeTab, setActiveTab] = useState('results');

  const parseTableData = (output) => {
    const lines = output.split('\n');
    const tableData = {
      headers: [],
      rows: [],
      meta: []
    };

    // Find the query results section
    const resultsIndex = lines.findIndex(line => line.startsWith('Query Results'));
    if (resultsIndex === -1) return null;

    // Extract metadata before the table
    tableData.meta = lines.slice(0, resultsIndex).filter(l => l.trim());

    // Find table boundaries
    const headerLineIndex = resultsIndex + 2;
    const separatorLineIndex = resultsIndex + 1;
    const endSeparatorIndex = lines.findIndex((line, idx) => 
      idx > separatorLineIndex && line.trim().match(/^-+$/)
    );

    // Extract headers
    tableData.headers = lines[headerLineIndex]
      .split(/\s{2,}/)
      .map(h => h.trim().replace(/^'(.*)'$/, '$1'))
      .filter(h => h);

    // Extract rows
    for (let i = headerLineIndex + 1; i < endSeparatorIndex; i++) {
      const line = lines[i].trim();
      if (!line || line.includes('---')) continue;

      const row = line.split(/\s{2,}/)
        .map(c => c.replace(/^'(.*)'$/, '$1').trim());
      
      if (row.length === tableData.headers.length) {
        tableData.rows.push(row);
      }
    }

    return tableData.rows.length > 0 ? tableData : null;
  };

  const executeQuery = async () => {
    const cleanCommand = query.trim().replace(/\n/g, ' ');
    if (!cleanCommand) return;
    
    setIsLoading(true);
    try {
      const response = await fetch('http://localhost:3001/execute', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ command: cleanCommand}), 
      });

      const data = await response.json();
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      
      setResults([data.output]);
      setHistory(prev => [...prev, { query, result: data.output }]);
      
      if (cleanCommand.toUpperCase().startsWith('USE DATABASE')) {
        setCurrentDb(cleanCommand.split(' ')[2]);
      }
    } catch (error) {
      console.error('Fetch error details:', error);
      setResults([`Network Error: ${error.message}`]);
    } finally {
      setIsLoading(false);
      setQuery('');
      setActiveTab('results');
    }
  };

  return (
    <div className="app-container">
      <div className="header">
        <div className="header-content">
          <div className="logo">
            <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"></path>
            </svg>
            <h1>SQL Query Interface</h1>
          </div>
          <div className="database-info">
            <span className="db-label">Database:</span>
            <span className="db-name">{currentDb}</span>
          </div>
        </div>
      </div>

      <div className="main-content">
        <div className="query-section">
          <div className="query-box">
            <div className="query-header">
              <h2>Query Editor</h2>
              <button 
                onClick={executeQuery}
                disabled={isLoading || !query.trim()}
                className="execute-btn"
              >
                {isLoading ? (
                  <>
                    <span className="spinner"></span>
                    Executing...
                  </>
                ) : (
                  <>
                    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                      <polygon points="5 3 19 12 5 21 5 3"></polygon>
                    </svg>
                    Execute
                  </>
                )}
              </button>
            </div>
            <textarea
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Enter SQL command (e.g. SELECT * FROM table)..."
              onKeyDown={(e) => {
                if (e.key === 'Enter' && e.ctrlKey) {
                  executeQuery();
                }
              }}
              disabled={isLoading}
            />
            <div className="query-footer">
              <small>Press Ctrl+Enter to execute</small>
            </div>
          </div>
        </div>

        <div className="results-section">
          <div className="results-tabs">
            <button 
              className={`tab-btn ${activeTab === 'results' ? 'active' : ''}`}
              onClick={() => setActiveTab('results')}
            >
              Results
            </button>
            <button 
              className={`tab-btn ${activeTab === 'history' ? 'active' : ''}`}
              onClick={() => setActiveTab('history')}
            >
              Query History
            </button>
          </div>

          <div className="results-content">
            {activeTab === 'results' ? (
              <div className="result-output">
                {results.length > 0 ? (
                  results.map((result, index) => {
                    const tableData = parseTableData(result);
                    
                    return (
                      <div key={index}>
                        {tableData ? (
                          <>
                            {tableData.meta.map((line, i) => (
                              <div key={`meta-${i}`} className="result-meta">
                                {line}
                              </div>
                            ))}
                            <div className="table-wrapper">
                              <table className="result-table">
                                <thead>
                                  <tr>
                                    {tableData.headers.map((header, i) => (
                                      <th key={i}>{header}</th>
                                    ))}
                                  </tr>
                                </thead>
                                <tbody>
                                  {tableData.rows.map((row, i) => (
                                    <tr key={i}>
                                      {row.map((cell, j) => (
                                        <td key={j} className={j > 0 && !isNaN(cell) ? 'numeric-cell' : ''}>
                                          {cell}
                                        </td>
                                      ))}
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            </div>
                          </>
                        ) : (
                          <pre>{result}</pre>
                        )}
                      </div>
                    );
                  })
                ) : (
                  <div className="empty-state">
                    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                      <circle cx="12" cy="12" r="10"></circle>
                      <line x1="12" y1="8" x2="12" y2="12"></line>
                      <line x1="12" y1="16" x2="12.01" y2="16"></line>
                    </svg>
                    <p>No results to display. Execute a query to see results here.</p>
                  </div>
                )}
              </div>
            ) : (
              <div className="history-list">
                {history.length > 0 ? (
                  history.map((entry, index) => {
                    const tableData = parseTableData(entry.result);
                    
                    return (
                      <div key={index} className="history-item">
                        <div className="history-query">
                          <span className="query-number">{index + 1}.</span>
                          {entry.query}
                        </div>
                        <div className="history-result">
                          {tableData ? (
                            <div className="table-wrapper">
                              <table className="result-table">
                                <thead>
                                  <tr>
                                    {tableData.headers.map((header, i) => (
                                      <th key={i}>{header}</th>
                                    ))}
                                  </tr>
                                </thead>
                                <tbody>
                                  {tableData.rows.slice(0, 3).map((row, i) => (
                                    <tr key={i}>
                                      {row.map((cell, j) => (
                                        <td key={j} className={j > 0 && !isNaN(cell) ? 'numeric-cell' : ''}>
                                          {cell}
                                        </td>
                                      ))}
                                    </tr>
                                  ))}
                                  {tableData.rows.length > 3 && (
                                    <tr>
                                      <td colSpan={tableData.headers.length} className="more-rows">
                                        + {tableData.rows.length - 3} more rows
                                      </td>
                                    </tr>
                                  )}
                                </tbody>
                              </table>
                            </div>
                          ) : (
                            <pre>{entry.result.split('\n').slice(0, 5).join('\n')}</pre>
                          )}
                        </div>
                      </div>
                    );
                  }).reverse()
                ) : (
                  <div className="empty-state">
                    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                      <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"></path>
                    </svg>
                    <p>Your query history will appear here</p>
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;