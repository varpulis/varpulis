import React from 'react';
import { BaseEdge, EdgeProps, getBezierPath } from '@xyflow/react';

const AnimatedEdge: React.FC<EdgeProps> = ({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  style = {},
  markerEnd,
}) => {
  const [edgePath] = getBezierPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
  });

  return (
    <>
      <BaseEdge
        id={id}
        path={edgePath}
        markerEnd={markerEnd}
        style={{
          ...style,
          strokeDasharray: 5,
          animation: 'dashed-animation 0.5s linear infinite',
        }}
      />
      <style>{`
        @keyframes dashed-animation {
          to {
            stroke-dashoffset: -10;
          }
        }
      `}</style>
    </>
  );
};

export default AnimatedEdge;