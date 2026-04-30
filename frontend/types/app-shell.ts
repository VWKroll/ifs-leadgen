export type TabKey = "event" | "cluster" | "sources" | "map" | "graph"; // "map" tab hidden from UI for this pass

export type AppSection = "event_intelligence" | "global_knowledge_graph" | "sales_tracker" | "settings";

export type ChatRequestContext = {
  active_tab?: TabKey | "global_graph";
  entity_id?: string;
  source_id?: string;
  graph_node_id?: string;
  region_id?: string;
  country_id?: string;
};

export type SharedChatContext = {
  selectedClusterId: string;
  selectedClusterName: string;
  contextTitle?: string;
  contextDescription?: string;
  contextPromptPrefix?: string;
  suggestedPrompts?: string[];
  contextChips?: string[];
  requestContext?: ChatRequestContext;
  defaultScope?: "selected_cluster" | "all";
  footerMeta?: Array<{ label: string; value: string }>;
};
