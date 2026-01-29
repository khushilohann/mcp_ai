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
      // Load the chat messages - use full message history if available
      const messages = item.messages && Array.isArray(item.messages) 
        ? item.messages 
        : [
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

  const handleMessageSent = (data) => {
    // If it's a new chat, update currentChatId
    if (data.isNewChat) {
      setCurrentChatId(data.chatId);
    }
  };

  const handleDeleteChat = (deletedIndex) => {
    // If deleted chat was the current one, clear it
    if (deletedIndex === currentChatId) {
      setCurrentChatId(null);
      if (chatRef.current && chatRef.current.clearChat) {
        chatRef.current.clearChat();
      }
    } else if (deletedIndex < currentChatId) {
      // If a chat before current was deleted, adjust the current chat ID
      setCurrentChatId(currentChatId - 1);
    }
  };

  return (
    <main className="main-container">
      <History 
        onSelect={handleHistorySelect} 
        onNewChat={handleNewChat}
        currentChatId={currentChatId}
        onDeleteChat={handleDeleteChat}
      />
      <div className="chat-wrapper">
        <Chat ref={chatRef} chatId={currentChatId} currentChatId={currentChatId} onMessageSent={handleMessageSent} />
      </div>
    </main>
  );
}
