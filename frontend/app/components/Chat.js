"use client";
import { useState, useRef, useEffect, forwardRef, useImperativeHandle } from 'react';

const Chat = forwardRef(function Chat({ chatId, onMessageSent }, ref) {
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState('');
  const [suggestions, setSuggestions] = useState([]);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const messagesEndRef = useRef(null);
  const inputRef = useRef(null);

  useImperativeHandle(ref, () => ({
    setInput: (text) => {
      setInput(text);
      inputRef.current?.focus();
    },
    loadChat: (chatMessages) => {
      setMessages(chatMessages || []);
    },
    clearChat: () => {
      setMessages([]);
      setInput('');
    }
  }));

  // Common query patterns for suggestions
  const commonPatterns = [
    'Show me all users',
    'Show me user with name',
    'Show me data from api path /users',
    'List all tables',
    'Export results to CSV',
    'Get user details from api path /users/20',
    'Show me all orders from last month',
    'What is the average order value by region?'
  ];

  useEffect(() => {
    if (!input) {
      setSuggestions([]);
      setShowSuggestions(false);
      return;
    }
    // Suggest from history and patterns
    const history = JSON.parse(localStorage.getItem('chat_history') || '[]');
    const historySuggestions = history
      .map(h => h.query)
      .filter(q => q.toLowerCase().includes(input.toLowerCase()))
      .slice(0, 3);
    const patternSuggestions = commonPatterns
      .filter(p => p.toLowerCase().includes(input.toLowerCase()))
      .slice(0, 2);
    const all = Array.from(new Set([...historySuggestions, ...patternSuggestions]));
    setSuggestions(all);
    setShowSuggestions(all.length > 0 && input.length > 0);
  }, [input]);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  useEffect(() => {
    // Load chat history if chatId is provided
    if (chatId !== undefined && chatId !== null) {
      const history = JSON.parse(localStorage.getItem('chat_history') || '[]');
      if (history[chatId]) {
        const chat = history[chatId];
        setMessages([
          { role: 'user', content: chat.query },
          { role: 'assistant', content: chat.response }
        ]);
      }
    }
  }, [chatId]);

  const sendMessage = async (e) => {
    e.preventDefault();
    if (!input.trim() || isLoading) return;
    
    setShowSuggestions(false);
    const userMessage = { role: 'user', content: input };
    setMessages((msgs) => [...msgs, userMessage]);
    const userInput = input;
    setInput('');
    setIsLoading(true);

    try {
      const res = await fetch('http://localhost:8000/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query: userInput })
      });
      
      if (!res.ok) throw new Error('Server error');
      
      const data = await res.json();
      const assistantMessage = { role: 'assistant', content: data.response };
      setMessages((msgs) => [...msgs, assistantMessage]);
      
      // Save to history
      const history = JSON.parse(localStorage.getItem('chat_history') || '[]');
      history.unshift({ 
        query: userInput, 
        response: data.response, 
        ts: Date.now(),
        messages: [userMessage, assistantMessage]
      });
      localStorage.setItem('chat_history', JSON.stringify(history.slice(0, 50)));
      
      // Trigger history update in parent
      window.dispatchEvent(new Event('storage'));
      
      if (onMessageSent) {
        onMessageSent({ query: userInput, response: data.response });
      }
    } catch (err) {
      setMessages((msgs) => [
        ...msgs,
        { role: 'assistant', content: 'Sorry, there was an error connecting to the backend. Please make sure the server is running.' }
      ]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleSuggestionClick = (suggestion) => {
    setInput(suggestion);
    setShowSuggestions(false);
    inputRef.current?.focus();
  };

  const handleKeyDown = (e) => {
    if (e.key === 'ArrowDown' && suggestions.length > 0) {
      e.preventDefault();
      // Could implement keyboard navigation here
    } else if (e.key === 'Escape') {
      setShowSuggestions(false);
    }
  };

  return (
    <div className="chat-container">
      <div className="chat-messages">
        {messages.length === 0 ? (
          <div className="empty-state">
            <h2>How can I help you today?</h2>
            <p>Start a conversation by typing a message below.</p>
          </div>
        ) : (
          messages.map((msg, idx) => (
            <div key={idx} className={`message-wrapper ${msg.role}`}>
              <div className="message-container">
                <div className={`message-avatar ${msg.role}`}>
                  {msg.role === 'user' ? 'U' : 'AI'}
                </div>
                <div className="message-content">
                  {msg.content}
                </div>
              </div>
            </div>
          ))
        )}
        {isLoading && (
          <div className="message-wrapper assistant">
            <div className="message-container">
              <div className="message-avatar assistant">AI</div>
              <div className="message-content">
                <span style={{ opacity: 0.7 }}>Thinking...</span>
              </div>
            </div>
          </div>
        )}
        <div ref={messagesEndRef} />
      </div>
      <div className="chat-input-container">
        <div className="chat-input-wrapper">
          {showSuggestions && suggestions.length > 0 && (
            <ul className="suggestion-list">
              {suggestions.map((s, i) => (
                <li
                  key={i}
                  className="suggestion-item"
                  onMouseDown={(e) => {
                    e.preventDefault();
                    handleSuggestionClick(s);
                  }}
                >
                  {s}
                </li>
              ))}
            </ul>
          )}
          <form className="chat-input-form" onSubmit={sendMessage} autoComplete="off">
            <textarea
              ref={inputRef}
              className="chat-input"
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder="Message..."
              rows={1}
              onFocus={() => setShowSuggestions(suggestions.length > 0 && input.length > 0)}
              onBlur={() => setTimeout(() => setShowSuggestions(false), 200)}
              style={{
                height: 'auto',
                minHeight: '24px',
                maxHeight: '200px',
                overflowY: 'auto'
              }}
              onInput={(e) => {
                e.target.style.height = 'auto';
                e.target.style.height = `${Math.min(e.target.scrollHeight, 200)}px`;
              }}
            />
            <button
              type="submit"
              className="send-button"
              disabled={!input.trim() || isLoading}
              title="Send message"
            >
              <svg width="20" height="20" viewBox="0 0 20 20" fill="none">
                <path
                  d="M18 2L9 11M18 2L12 18L9 11M18 2L2 8L9 11"
                  stroke="currentColor"
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
              </svg>
            </button>
          </form>
        </div>
      </div>
    </div>
  );
});

export default Chat;
