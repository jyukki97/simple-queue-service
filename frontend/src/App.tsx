import React, { useState } from 'react';
import QueueManager from './components/QueueManager';
import MessageManager from './components/MessageManager';
import './App.css';

function App() {
  const [selectedQueue, setSelectedQueue] = useState<string>('');

  const handleQueueChange = (queueName: string) => {
    setSelectedQueue(queueName);
  };

  return (
    <div className="App">
      <header className="App-header">
        <h1>Simple Queue Service (SQS) 테스트 도구</h1>
        <p>AWS SQS와 유사한 경량 메시지 큐 시스템</p>
      </header>
      
      <main className="App-main">
        <div className="container">
          <div className="section">
            <h2>큐 관리</h2>
            <QueueManager 
              onQueueSelect={handleQueueChange}
              selectedQueue={selectedQueue || null}
            />
          </div>
          
          {selectedQueue && (
            <div className="section">
              <h2>메시지 관리 - {selectedQueue}</h2>
              <MessageManager queueName={selectedQueue} />
            </div>
          )}
          
          {!selectedQueue && (
            <div className="section">
              <div className="placeholder">
                <h3>큐를 선택하세요</h3>
                <p>위에서 큐를 생성하거나 기존 큐를 선택하면 메시지 관리 기능을 사용할 수 있습니다.</p>
              </div>
            </div>
          )}
        </div>
      </main>
      
      <footer className="App-footer">
        <p>Simple Queue Service - Kotlin/Spring Boot + React</p>
      </footer>
    </div>
  );
}

export default App;
