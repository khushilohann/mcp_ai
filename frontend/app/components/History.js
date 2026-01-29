import { useState, useEffect } from 'react';

export default function History({ onSelect }) {
  const [history, setHistory] = useState([]);

  useEffect(() => {
    const stored = localStorage.getItem('chat_history');
    if (stored) setHistory(JSON.parse(stored));
  }, []);

  const handleSelect = (item) => {
    if (onSelect) onSelect(item);
  };

  return (
    <aside className="history-sidebar">
      <h3>History</h3>
      <ul>
        {history.map((item, idx) => (
          <li key={idx} onClick={() => handleSelect(item)}>
            <span>{item.query.slice(0, 40)}...</span>
          </li>
        ))}
      </ul>
    </aside>
  );
}
