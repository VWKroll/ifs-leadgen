"use client";

import { CSSProperties, useEffect, useMemo } from "react";
import {
  Background,
  Controls,
  Edge,
  EdgeChange,
  Handle,
  MarkerType,
  MiniMap,
  Node,
  NodeChange,
  NodeProps,
  Position,
  ReactFlow,
  ReactFlowProvider,
  useEdgesState,
  useNodesState,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";

import { colorForBranch } from "@/lib/semantics";
import { GraphEdge, GraphNode } from "@/types/api";

type Props = {
  nodes: GraphNode[];
  edges: GraphEdge[];
  selectedNodeId?: string;
  expanded?: boolean;
  onSelectNode?: (nodeId: string) => void;
};

type GraphNodeData = {
  label: string;
  original: GraphNode;
  selected: boolean;
};

function graphNodeLabel(node: GraphNode): string {
  if (node.type === "cluster") {
    const headline = typeof node.detail?.headline === "string" ? node.detail.headline.trim() : "";
    if (headline) return headline;
  }

  return node.label;
}

function graphNodeStyle(node: GraphNode, selected: boolean): CSSProperties {
  return {
    width: node.type === "cluster" ? 320 : node.type === "company" ? 176 : 144,
    minHeight: node.type === "cluster" ? 116 : node.type === "company" ? 92 : 78,
    borderRadius: node.type === "cluster" ? 28 : 22,
    background: nodeColor(node),
    color: "#f8fafc",
    border: selected ? "2px solid rgba(255,255,255,0.9)" : "1px solid rgba(255,255,255,0.16)",
    boxShadow: selected
      ? "0 24px 52px rgba(0,0,0,0.34), 0 0 0 8px rgba(63,146,255,0.16), inset 0 0 30px rgba(255,255,255,0.06)"
      : "0 24px 48px rgba(0,0,0,0.26), inset 0 0 30px rgba(255,255,255,0.04)",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    textAlign: "center",
    padding: "14px 16px",
    fontWeight: 600,
    lineHeight: node.type === "cluster" ? 1.4 : 1.3,
  };
}

function GraphNodeCard({ data }: NodeProps<Node<GraphNodeData>>) {
  const node = data.original;
  const hiddenHandleStyle: CSSProperties = {
    width: 10,
    height: 10,
    opacity: 0,
    background: "transparent",
    border: 0,
    pointerEvents: "none",
  };

  return (
    <>
      <Handle id="target-left" type="target" position={Position.Left} style={hiddenHandleStyle} />
      <Handle id="target-top" type="target" position={Position.Top} style={hiddenHandleStyle} />
      <div style={graphNodeStyle(node, data.selected)}>{data.label}</div>
      <Handle id="source-right" type="source" position={Position.Right} style={hiddenHandleStyle} />
      <Handle id="source-bottom" type="source" position={Position.Bottom} style={hiddenHandleStyle} />
    </>
  );
}

function buildLayout(nodes: GraphNode[]) {
  const centerX = 560;
  const centerY = 340;
  const spaced: Record<string, { x: number; y: number }> = {};
  const cluster = nodes.find((node) => node.type === "cluster");
  if (cluster) spaced[cluster.id] = { x: centerX, y: centerY };

  const companies = nodes.filter((node) => node.type === "company");
  const roles = nodes.filter((node) => node.type === "role_track");
  const peer = companies.filter((node) => node.branch_type === "peer");
  const ownership = companies.filter((node) => node.branch_type === "ownership");
  const direct = companies.filter((node) => node.branch_type === "direct");

  const placeVertical = (items: GraphNode[], x: number, startY: number, gap: number) => {
    items.forEach((item, index) => {
      spaced[item.id] = { x, y: startY + index * gap };
    });
  };

  placeVertical(peer, 150, 180, 164);
  placeVertical(ownership, 970, 180, 164);
  direct.forEach((item, index) => {
    spaced[item.id] = { x: 360 + index * 220, y: 92 };
  });

  roles.forEach((role, index) => {
    const company = companies.find((item) => item.id === `entity:${role.entity_id ?? ""}`);
    const base = company ? spaced[company.id] : { x: centerX, y: centerY };
    const branch = company?.branch_type;
    if (branch === "peer") spaced[role.id] = { x: base.x - 190, y: base.y + (index % 2) * 88 };
    else if (branch === "ownership") spaced[role.id] = { x: base.x + 190, y: base.y + (index % 2) * 88 };
    else spaced[role.id] = { x: base.x, y: base.y - 138 - index * 18 };
  });

  return spaced;
}

function nodeColor(node: GraphNode) {
  if (node.type === "cluster") return "var(--graph-cluster)";
  if (node.type === "role_track") return "var(--graph-role-track)";
  return colorForBranch(node.branch_type as "direct" | "peer" | "ownership" | null | undefined);
}

function edgeColor(edge: GraphEdge) {
  return colorForBranch(edge.branch_type as "direct" | "peer" | "ownership" | null | undefined);
}

function buildFlowNodes(nodes: GraphNode[], selectedNodeId?: string): Node[] {
  const layout = buildLayout(nodes);
  return nodes.map((node) => {
    const isSelected = node.id === selectedNodeId;
    return {
      id: node.id,
      type: "graphNode",
      position: layout[node.id] ?? { x: 0, y: 0 },
      data: { label: graphNodeLabel(node), original: node, selected: isSelected },
      draggable: true,
    };
  });
}

function buildFlowEdges(edges: GraphEdge[]): Edge[] {
  return edges.map((edge) => ({
    id: edge.id,
    source: edge.source,
    target: edge.target,
    sourceHandle: "source-right",
    targetHandle: "target-left",
    label: edge.label ?? undefined,
    selectable: false,
    type: "smoothstep",
    animated: edge.branch_type === "direct",
    markerEnd: {
      type: MarkerType.ArrowClosed,
      width: 18,
      height: 18,
      color: edgeColor(edge),
    },
    style: {
      stroke: edgeColor(edge),
      strokeWidth: 3.5,
      strokeDasharray: edge.branch_type === "peer" ? "10 8" : edge.branch_type === "ownership" ? "4 8" : undefined,
    },
    labelStyle: { fill: "#94a3b8", fontSize: 11 },
  }));
}

export function OpportunityGraph({ nodes, edges, selectedNodeId, expanded = false, onSelectNode }: Props) {
  const initialNodes = useMemo(() => buildFlowNodes(nodes, selectedNodeId), [nodes, selectedNodeId]);
  const initialEdges = useMemo(() => buildFlowEdges(edges), [edges]);
  const [flowNodes, setFlowNodes, onNodesChange] = useNodesState(initialNodes);
  const [flowEdges, setFlowEdges, onEdgesChange] = useEdgesState(initialEdges);
  const nodeTypes = useMemo(() => ({ graphNode: GraphNodeCard }), []);

  useEffect(() => {
    setFlowNodes((current) => {
      const currentPositions = new Map(current.map((node) => [node.id, node.position]));
      return buildFlowNodes(nodes, selectedNodeId).map((node) => ({
        ...node,
        position: currentPositions.get(node.id) ?? node.position,
      }));
    });
  }, [nodes, selectedNodeId, setFlowNodes]);

  useEffect(() => {
    setFlowEdges(buildFlowEdges(edges));
  }, [edges, setFlowEdges]);

  return (
    <div className={`panel canvas ${expanded ? "canvasExpanded" : ""}`}>
      <ReactFlowProvider>
        <ReactFlow
          nodes={flowNodes}
          edges={flowEdges}
          nodeTypes={nodeTypes}
          fitView
          fitViewOptions={{ padding: 0.16 }}
          minZoom={0.35}
          maxZoom={2.4}
          nodesDraggable
          nodesConnectable={false}
          elementsSelectable
          onNodesChange={onNodesChange as (changes: NodeChange[]) => void}
          onEdgesChange={onEdgesChange as (changes: EdgeChange[]) => void}
          onNodeClick={(_, node) => onSelectNode?.(node.id)}
        >
          <MiniMap />
          <Controls />
          <Background color="#1d2b3f" gap={28} />
        </ReactFlow>
      </ReactFlowProvider>
    </div>
  );
}
