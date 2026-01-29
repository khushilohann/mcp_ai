"use client";
import { useState, useEffect } from 'react';

export default function History({ onSelect, onNewChat, currentChatId }) {
  const [history, setHistory] = useState([]);
  const [collapsed, setCollapsed] = useState(false);

  useEffect(() => {
    loadHistory();
    // Listen for storage changes from other tabs/components
    const handleStorageChange = () => loadHistory();
    window.addEventListener('storage', handleStorageChange);
    return () => window.removeEventListener('storage', handleStorageChange);
  }, []);

  const loadHistory = () => {
    const stored = localStorage.getItem('chat_history');
    if (stored) {
      try {
        const parsed = JSON.parse(stored);
        setHistory(parsed);
      } catch (e) {
        console.error('Error parsing history:', e);
      }
    }
  };

  const handleSelect = (item, index) => {
    if (onSelect) onSelect(item, index);
  };

  const formatDate = (timestamp) => {
    const date = new Date(timestamp);
    const now = new Date();
    const diff = now - date;
    const days = Math.floor(diff / (1000 * 60 * 60 * 24));
    
    if (days === 0) return 'Today';
    if (days === 1) return 'Yesterday';
    if (days < 7) return `${days} days ago`;
    
    return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
  };

  const groupedHistory = history.reduce((acc, item, index) => {
    const date = formatDate(item.ts || Date.now());
    if (!acc[date]) acc[date] = [];
    acc[date].push({ ...item, index });
    return acc;
  }, {});

  return (
    <aside className={`history-sidebar ${collapsed ? 'collapsed' : ''}`}>
      <div className="sidebar-header">
        <button 
          className="new-chat-btn" 
          onClick={onNewChat}
          title="New chat"
        >
          <span className="icon">+</span>
          <span>New chat</span>
        </button>
        <button 
          className="sidebar-toggle"
          onClick={() => setCollapsed(!collapsed)}
          title={collapsed ? "Expand sidebar" : "Collapse sidebar"}
        >
          <span>{collapsed ? '→' : '←'}</span>
        </button>
      </div>
      <div className="history-content">
        {Object.keys(groupedHistory).length === 0 ? (
          <div style={{ padding: '20px', textAlign: 'center', color: 'var(--text-secondary)', fontSize: '14px' }}>
            No chat history
          </div>
        ) : (
          Object.entries(groupedHistory).map(([date, items]) => (
            <div key={date}>
              <div className="history-title">{date}</div>
              <ul className="history-list">
                {items.map((item) => (
                  <li
                    key={item.index}
                    className={`history-item ${currentChatId === item.index ? 'active' : ''}`}
                    onClick={() => handleSelect(item, item.index)}
                    title={item.query}
                  >
                    <span className="history-item-icon">💬</span>
                    <span className="history-item-text">
                      {item.query.length > 30 ? `${item.query.slice(0, 30)}...` : item.query}
                    </span>
                  </li>
                ))}
              </ul>
            </div>
          ))
        )}
      </div>
    </aside>
  );
}
