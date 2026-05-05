/**
 * Complete color/size maps for all 26 node types and 16 edge types.
 * Domain groups for sidebar organization.
 */

// ── Node styles ──
export const NODE_STYLES = {
  // AWS Infrastructure (orange/amber)
  aws_account:          { color: "#f97316", size: 20 },
  ecs_cluster:          { color: "#ea580c", size: 16 },
  ecs_service:          { color: "#fb923c", size: 12 },
  ecs_task_definition:  { color: "#fdba74", size: 10 },
  s3_bucket:            { color: "#f59e0b", size: 16 },
  s3_prefix:            { color: "#fbbf24", size: 12 },
  s3_object:            { color: "#fde68a", size: 7  },
  image_repository:     { color: "#d97706", size: 14 },
  image:                { color: "#fcd34d", size: 10 },

  // Code (green/teal)
  codebase:   { color: "#22c55e", size: 16 },
  file:       { color: "#10b981", size: 7  },
  class:      { color: "#84cc16", size: 10 },
  function:   { color: "#14b8a6", size: 6  },
  package_manifest: { color: "#0891b2", size: 10 },
  dependency: { color: "#06b6d4", size: 8  },

  // Data (blue/indigo)
  database:    { color: "#6366f1", size: 18 },
  schema:      { color: "#8b5cf6", size: 14 },
  table:       { color: "#0ea5e9", size: 10 },
  column:      { color: "#94a3b8", size: 4  },
  rds_cluster: { color: "#3b82f6", size: 16 },

  // Security (red/rose)
  security_group: { color: "#ef4444", size: 14 },
  nacl:           { color: "#f43f5e", size: 12 },
  vpc:            { color: "#dc2626", size: 16 },

  // Identity (purple/fuchsia)
  iam_role:   { color: "#a855f7", size: 14 },
  iam_user:   { color: "#c084fc", size: 12 },
  iam_policy: { color: "#d946ef", size: 10 },
  identity:   { color: "#e879f9", size: 12 },
  sso_group:  { color: "#f0abfc", size: 12 },

  // Fallback
  unknown: { color: "#64748b", size: 5 },
};

// ── Edge styles ──
export const EDGE_STYLES = {
  contains:           { color: "#45475a", size: 0.5 },
  hosts:              { color: "#94a3b8", size: 0.5 },
  calls:              { color: "#a78bfa", size: 1.5 },
  instantiates:       { color: "#c084fc", size: 1.5 },
  depends_on:         { color: "#22d3ee", size: 1   },
  builds:             { color: "#38bdf8", size: 1.5 },
  executes:           { color: "#facc15", size: 2   },
  reads:              { color: "#f97316", size: 2   },
  writes:             { color: "#ef4444", size: 2   },
  models:             { color: "#fb923c", size: 1.5 },
  references:         { color: "#f43f5e", size: 1.5 },
  soft_reference:     { color: "#f9a8d4", size: 1.5 },
  allows_traffic_to:  { color: "#34d399", size: 1.5 },
  assumes:            { color: "#a78bfa", size: 1.5 },
  attaches:           { color: "#818cf8", size: 1   },
  member_of:          { color: "#e879f9", size: 1   },
  protected_by:       { color: "#fb7185", size: 1.5 },
};

// ── Domain groups for sidebar organization ──
export const DOMAIN_GROUPS = {
  "AWS Infrastructure": [
    "aws_account", "ecs_cluster", "ecs_service", "ecs_task_definition",
    "s3_bucket", "s3_prefix", "s3_object", "image_repository", "image",
  ],
  "Code": [
    "codebase", "file", "class", "function", "package_manifest", "dependency",
  ],
  "Data": [
    "database", "schema", "table", "column", "rds_cluster",
  ],
  "Security": [
    "security_group", "nacl", "vpc",
  ],
  "Identity": [
    "iam_role", "iam_user", "iam_policy", "identity", "sso_group",
  ],
};

export const STRUCTURAL_SOURCES = new Set([
  "database", "schema", "codebase", "file",
]);
