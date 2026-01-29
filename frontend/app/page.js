"use client";

import Chat from './components/Chat';
import History from './components/History';
import { useRef, useState } from 'react';

export default function Home() {
  const chatRef = useRef();
  const [currentChatId, setCurrentChatId] = useState(null);

  const handleHistorySelect = (item, index) => {
    setCurrentChatId(index);
    if (chatRef.current && chatRef.current.loadChat) {
      // Load the chat messages
      const messages = [
        { role: 'user', content: item.query },
        { role: 'assistant', content: item.response }
      ];
      chatRef.current.loadChat(messages);
    }
  };

  const handleNewChat = () => {
    setCurrentChatId(null);
    if (chatRef.current && chatRef.current.clearChat) {
      chatRef.current.clearChat();
    }
  };

  return (
    <main className="main-container">
      <History 
        onSelect={handleHistorySelect} 
        onNewChat={handleNewChat}
        currentChatId={currentChatId}
      />
      <div className="chat-wrapper">
        <Chat ref={chatRef} chatId={currentChatId} />
      </div>
    </main>
  );
}
