import React from 'react';

interface SidebarItem {
  type: string;
  subtype?: string;
  label: string;
  icon: string;
  description: string;
}

const nodeCategories: { title: string; items: SidebarItem[] }[] = [
  {
    title: 'Connectors',
    items: [
      { type: 'connector', subtype: 'mqtt', label: 'MQTT', icon: '📡', description: 'MQTT broker connection' },
      { type: 'connector', subtype: 'kafka', label: 'Kafka', icon: '📨', description: 'Kafka cluster connection' },
      { type: 'connector', subtype: 'http', label: 'HTTP', icon: '🌐', description: 'HTTP/REST endpoint' },
      { type: 'connector', subtype: 'amqp', label: 'AMQP', icon: '🐰', description: 'RabbitMQ/AMQP connection' },
      { type: 'connector', subtype: 'file', label: 'File', icon: '📁', description: 'File system' },
    ],
  },
  {
    title: 'Data Flow',
    items: [
      { type: 'source', label: 'Source', icon: '📥', description: 'Read from connector' },
      { type: 'stream', label: 'Stream', icon: '🌊', description: 'Process & transform data' },
      { type: 'emit', label: 'Emit', icon: '📤', description: 'Shape output data' },
      { type: 'sink', subtype: 'topic', label: 'Sink (Topic)', icon: '📮', description: 'Write to connector topic' },
      { type: 'sink', subtype: 'console', label: 'Console', icon: '💻', description: 'Print to console' },
    ],
  },
  {
    title: 'Patterns',
    items: [
      { type: 'pattern', subtype: 'SEQ', label: 'Sequence', icon: '➡️', description: 'Event sequence pattern' },
      { type: 'pattern', subtype: 'AND', label: 'All (AND)', icon: '&', description: 'All events must occur' },
      { type: 'pattern', subtype: 'OR', label: 'Any (OR)', icon: '|', description: 'Any event matches' },
    ],
  },
  {
    title: 'Types',
    items: [
      { type: 'event', label: 'Event Type', icon: '📋', description: 'Define event structure' },
    ],
  },
];

const Sidebar: React.FC = () => {
  const onDragStart = (event: React.DragEvent, item: SidebarItem) => {
    const dragData = JSON.stringify({ type: item.type, subtype: item.subtype });
    event.dataTransfer.setData('application/reactflow', dragData);
    event.dataTransfer.effectAllowed = 'move';
  };

  return (
    <div className="sidebar">
      {nodeCategories.map((category) => (
        <div key={category.title} className="sidebar-category">
          <h3>{category.title}</h3>
          {category.items.map((item) => (
            <div
              key={`${item.type}-${item.subtype || ''}`}
              className={`sidebar-item ${item.type}`}
              draggable
              onDragStart={(e) => onDragStart(e, item)}
              title={item.description}
            >
              <span className="icon">{item.icon}</span>
              <span className="label">{item.label}</span>
            </div>
          ))}
        </div>
      ))}
    </div>
  );
};

export default Sidebar;
