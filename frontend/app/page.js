"use client";

import Chat from './components/Chat';
import History from './components/History';
import { useRef } from 'react';

export default function Home() {
  const chatRef = useRef();
  const handleHistorySelect = (item) => {
    if (chatRef.current && chatRef.current.setInput) {
      chatRef.current.setInput(item.query);
    }
  };
  return (
    <main className="main-container" style={{ display: 'flex' }}>
      <History onSelect={handleHistorySelect} />
      <div style={{ flex: 1 }}>
        <Chat ref={chatRef} />
      </div>
    </main>
  );
}
