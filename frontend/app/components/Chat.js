"use client";
import { useState, useRef, useEffect } from 'react';

export default function Chat() {
  const [messages, setMessages] = useState([
    { role: 'assistant', content: 'Hello! How can I help you today?' }
  ]);
  const [input, setInput] = useState('');
  const [suggestions, setSuggestions] = useState([]);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const messagesEndRef = useRef(null);
  // Common query patterns for suggestions
  const commonPatterns = [
    'Show me all users',
    'Show me user with name ',
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
      .filter(q => q.toLowerCase().startsWith(input.toLowerCase()));
    const patternSuggestions = commonPatterns.filter(p => p.toLowerCase().startsWith(input.toLowerCase()));
    const all = Array.from(new Set([...historySuggestions, ...patternSuggestions])).slice(0, 5);
    setSuggestions(all);
    setShowSuggestions(all.length > 0);
  }, [input]);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  const sendMessage = async (e) => {
    e.preventDefault();
    if (!input.trim()) return;
    setShowSuggestions(false);
    const userMessage = { role: 'user', content: input };
    setMessages((msgs) => [...msgs, userMessage]);
    const userInput = input;
    setInput('');
    try {
      const res = await fetch('http://localhost:8000/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query: userInput })
      });
      if (!res.ok) throw new Error('Server error');
      const data = await res.json();
      setMessages((msgs) => [
        ...msgs,
        { role: 'assistant', content: data.response }
      ]);
      // Save to history
      const history = JSON.parse(localStorage.getItem('chat_history') || '[]');
      history.unshift({ query: userInput, response: data.response, ts: Date.now() });
      localStorage.setItem('chat_history', JSON.stringify(history.slice(0, 50)));
    } catch (err) {
      setMessages((msgs) => [
        ...msgs,
        { role: 'assistant', content: 'Sorry, there was an error connecting to the backend.' }
      ]);
    }
  };

  return (
    <div className="chat-container">
      <div className="chat-messages">
        {messages.map((msg, idx) => (
          <div key={idx} className={`chat-message ${msg.role}`}> 
            <span>{msg.content}</span>
          </div>
        ))}
        <div ref={messagesEndRef} />
      </div>
      <form className="chat-input" onSubmit={sendMessage} autoComplete="off">
        <div style={{ position: 'relative', width: '100%' }}>
          <input
            type="text"
            value={input}
            onChange={e => setInput(e.target.value)}
            placeholder="Type your message..."
            onFocus={() => setShowSuggestions(suggestions.length > 0)}
            onBlur={() => setTimeout(() => setShowSuggestions(false), 100)}
            autoComplete="off"
          />
          {showSuggestions && (
            <ul className="suggestion-list" style={{ position: 'absolute', left: 0, right: 0, top: '100%', background: '#23232a', color: '#fff', zIndex: 10, borderRadius: 8, margin: 0, padding: 0, listStyle: 'none', boxShadow: '0 2px 8px #000a' }}>
              {suggestions.map((s, i) => (
                <li key={i} style={{ padding: '8px 12px', cursor: 'pointer' }} onMouseDown={() => { setInput(s); setShowSuggestions(false); }}>{s}</li>
              ))}
            </ul>
          )}
        </div>
        <button type="submit">Send</button>
      </form>
    </div>
  );
}
