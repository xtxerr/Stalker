# Entity View System - Implementierungsplan

**Version:** 1.0
**Erstellt:** 2025-01-17
**Status:** PLANUNG

---

## Übersicht

Dieses Dokument beschreibt die vollständige Implementierung eines Entity-First View Systems für die Stalker Monitoring Platform. Das System ermöglicht Benutzern, dynamische und statische Views zu erstellen, die in der Datenbank persistiert werden.

### Kernprinzipien

1. **Everything is an Entity** - Targets, Pollers, Views, Folders, Queries sind alle Entities
2. **Unified Storage** - Eine `entities` Tabelle für alles
3. **JSONB Specs** - Flexible Konfiguration ohne Schema-Änderungen
4. **Real-time Updates** - WebSocket-basierte Live-Updates
5. **User Ownership** - Jeder User hat eigene Workspaces/Views

---

## Implementierungsphasen

```
Phase 1: Entity Store Foundation          [  ] 0%
Phase 2: Entity Types & Validation        [  ] 0%
Phase 3: Query Language (UQL)             [  ] 0%
Phase 4: View Rendering Engine            [  ] 0%
Phase 5: REST API                         [  ] 0%
Phase 6: Real-time Subscriptions          [  ] 0%
Phase 7: Migration & Integration          [  ] 0%
```

---

## Phase 1: Entity Store Foundation

**Ziel:** Basis-Infrastruktur für das Entity-System

### 1.1 Database Schema

**Status:** [ ] TODO

**Datei:** `internal/store/schema_entities.go`

```sql
-- Haupt-Entity-Tabelle
CREATE TABLE entities (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    kind VARCHAR(64) NOT NULL,
    namespace VARCHAR(128) NOT NULL,
    name VARCHAR(256) NOT NULL,

    -- Ownership
    owner VARCHAR(256),
    visibility VARCHAR(32) DEFAULT 'private',

    -- Data
    labels JSONB DEFAULT '{}',
    annotations JSONB DEFAULT '{}',
    spec JSONB NOT NULL DEFAULT '{}',
    status JSONB DEFAULT '{}',

    -- Hierarchy
    parent_id UUID REFERENCES entities(id) ON DELETE CASCADE,
    path TEXT[],
    sort_order INTEGER DEFAULT 0,

    -- Versioning
    version INTEGER DEFAULT 1,
    generation INTEGER DEFAULT 1,

    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    deleted_at TIMESTAMPTZ,

    -- Sync
    source VARCHAR(32) DEFAULT 'api',
    source_hash BYTEA,
    synced_at TIMESTAMPTZ,

    UNIQUE (kind, namespace, name) WHERE deleted_at IS NULL
);

-- Indices
CREATE INDEX idx_entities_kind ON entities(kind) WHERE deleted_at IS NULL;
CREATE INDEX idx_entities_namespace ON entities(namespace) WHERE deleted_at IS NULL;
CREATE INDEX idx_entities_owner ON entities(owner) WHERE deleted_at IS NULL;
CREATE INDEX idx_entities_parent ON entities(parent_id) WHERE deleted_at IS NULL;
CREATE INDEX idx_entities_path ON entities USING GIN(path) WHERE deleted_at IS NULL;
CREATE INDEX idx_entities_labels ON entities USING GIN(labels) WHERE deleted_at IS NULL;

-- Relations-Tabelle
CREATE TABLE relations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    from_kind VARCHAR(64) NOT NULL,
    from_namespace VARCHAR(128) NOT NULL,
    from_name VARCHAR(256) NOT NULL,
    to_kind VARCHAR(64) NOT NULL,
    to_namespace VARCHAR(128) NOT NULL,
    to_name VARCHAR(256) NOT NULL,
    relation_type VARCHAR(64) NOT NULL,
    properties JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT now(),
    created_by VARCHAR(256),

    UNIQUE (from_kind, from_namespace, from_name,
            to_kind, to_namespace, to_name, relation_type)
);

CREATE INDEX idx_relations_from ON relations(from_kind, from_namespace, from_name);
CREATE INDEX idx_relations_to ON relations(to_kind, to_namespace, to_name);

-- History-Tabelle (optional, für Undo)
CREATE TABLE entity_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id UUID NOT NULL,
    version INTEGER NOT NULL,
    kind VARCHAR(64) NOT NULL,
    namespace VARCHAR(128) NOT NULL,
    name VARCHAR(256) NOT NULL,
    spec JSONB NOT NULL,
    labels JSONB,
    changed_at TIMESTAMPTZ DEFAULT now(),
    changed_by VARCHAR(256),
    change_type VARCHAR(32),

    UNIQUE (entity_id, version)
);
```

**Tasks:**
- [ ] 1.1.1 Schema-Migration erstellen (`internal/store/migrations/003_entities.sql`)
- [ ] 1.1.2 DuckDB-kompatibles Schema testen
- [ ] 1.1.3 Indices für Performance optimieren

### 1.2 Entity Store Interface

**Status:** [ ] TODO

**Datei:** `internal/store/entity.go`

```go
// Entity repräsentiert ein generisches Entity
type Entity struct {
    ID          string            `json:"id"`
    Kind        string            `json:"kind"`
    Namespace   string            `json:"namespace"`
    Name        string            `json:"name"`
    Owner       string            `json:"owner,omitempty"`
    Visibility  string            `json:"visibility,omitempty"`
    Labels      map[string]string `json:"labels,omitempty"`
    Annotations map[string]string `json:"annotations,omitempty"`
    Spec        json.RawMessage   `json:"spec"`
    Status      json.RawMessage   `json:"status,omitempty"`
    ParentID    *string           `json:"parent_id,omitempty"`
    Path        []string          `json:"path,omitempty"`
    SortOrder   int               `json:"sort_order"`
    Version     int               `json:"version"`
    Generation  int               `json:"generation"`
    CreatedAt   time.Time         `json:"created_at"`
    UpdatedAt   time.Time         `json:"updated_at"`
    DeletedAt   *time.Time        `json:"deleted_at,omitempty"`
    Source      string            `json:"source,omitempty"`
}

// EntityRef ist eine Referenz auf ein Entity
type EntityRef struct {
    Kind      string `json:"kind"`
    Namespace string `json:"namespace"`
    Name      string `json:"name"`
}

// Relation repräsentiert eine Beziehung zwischen Entities
type Relation struct {
    ID           string            `json:"id"`
    From         EntityRef         `json:"from"`
    To           EntityRef         `json:"to"`
    RelationType string            `json:"relation_type"`
    Properties   map[string]any    `json:"properties,omitempty"`
    CreatedAt    time.Time         `json:"created_at"`
    CreatedBy    string            `json:"created_by,omitempty"`
}
```

**Tasks:**
- [ ] 1.2.1 Entity struct definieren
- [ ] 1.2.2 EntityRef struct definieren
- [ ] 1.2.3 Relation struct definieren
- [ ] 1.2.4 JSON Marshaling/Unmarshaling implementieren

### 1.3 Entity Store CRUD Operations

**Status:** [ ] TODO

**Datei:** `internal/store/entity_ops.go`

```go
// EntityStore Interface
type EntityStore interface {
    // CRUD
    Create(ctx context.Context, entity *Entity) error
    Get(ctx context.Context, ref EntityRef) (*Entity, error)
    Update(ctx context.Context, entity *Entity) error
    Delete(ctx context.Context, ref EntityRef, hard bool) error

    // Queries
    List(ctx context.Context, opts ListOptions) ([]*Entity, error)
    Count(ctx context.Context, opts ListOptions) (int, error)

    // Hierarchy
    GetChildren(ctx context.Context, parentID string) ([]*Entity, error)
    GetAncestors(ctx context.Context, entityID string) ([]*Entity, error)
    Move(ctx context.Context, entityID string, newParentID *string) error

    // Relations
    CreateRelation(ctx context.Context, rel *Relation) error
    DeleteRelation(ctx context.Context, relID string) error
    GetRelations(ctx context.Context, ref EntityRef, relType string) ([]*Relation, error)

    // Bulk
    BulkCreate(ctx context.Context, entities []*Entity) error
    BulkUpdate(ctx context.Context, entities []*Entity) error
    BulkDelete(ctx context.Context, refs []EntityRef) error

    // History
    GetHistory(ctx context.Context, ref EntityRef, limit int) ([]*EntityHistory, error)
}

// ListOptions für Entity-Queries
type ListOptions struct {
    Kind       string
    Namespace  string
    Owner      string
    ParentID   *string
    Labels     map[string]string
    Filter     *FilterExpression
    OrderBy    []OrderSpec
    Limit      int
    Offset     int
    IncludeDeleted bool
}
```

**Tasks:**
- [ ] 1.3.1 Create implementieren
- [ ] 1.3.2 Get implementieren
- [ ] 1.3.3 Update mit Optimistic Locking implementieren
- [ ] 1.3.4 Delete (soft + hard) implementieren
- [ ] 1.3.5 List mit Filterung implementieren
- [ ] 1.3.6 Hierarchy-Operationen implementieren
- [ ] 1.3.7 Relation-Operationen implementieren
- [ ] 1.3.8 Bulk-Operationen implementieren
- [ ] 1.3.9 History-Tracking implementieren

### 1.4 Entity Store Tests

**Status:** [ ] TODO

**Datei:** `internal/store/entity_test.go`

**Tasks:**
- [ ] 1.4.1 CRUD Tests
- [ ] 1.4.2 Concurrency Tests (Optimistic Locking)
- [ ] 1.4.3 Hierarchy Tests
- [ ] 1.4.4 Relation Tests
- [ ] 1.4.5 Performance Tests (1000+ Entities)

---

## Phase 2: Entity Types & Validation

**Ziel:** Typisierte Entities mit Schema-Validierung

### 2.1 Kind Registry

**Status:** [ ] TODO

**Datei:** `internal/entity/registry.go`

```go
// KindSpec definiert ein Entity-Kind
type KindSpec struct {
    Kind        string
    Singular    string
    Plural      string
    Description string

    // Schema
    SpecSchema   *jsonschema.Schema
    StatusSchema *jsonschema.Schema

    // Behavior
    Namespaced  bool
    Owned       bool  // Hat Owner
    Hierarchical bool // Kann Parent haben

    // Hooks
    OnCreate    func(ctx context.Context, e *Entity) error
    OnUpdate    func(ctx context.Context, old, new *Entity) error
    OnDelete    func(ctx context.Context, e *Entity) error

    // Validation
    Validate    func(e *Entity) error
}

// Registry hält alle registrierten Kinds
type Registry struct {
    kinds map[string]*KindSpec
}

func NewRegistry() *Registry
func (r *Registry) Register(spec *KindSpec) error
func (r *Registry) Get(kind string) (*KindSpec, bool)
func (r *Registry) Validate(e *Entity) error
func (r *Registry) List() []*KindSpec
```

**Tasks:**
- [ ] 2.1.1 Registry implementieren
- [ ] 2.1.2 JSON Schema Validierung integrieren
- [ ] 2.1.3 Hooks-System implementieren

### 2.2 Core Entity Kinds

**Status:** [ ] TODO

**Dateien:** `internal/entity/kinds/`

#### 2.2.1 Workspace Kind

**Datei:** `internal/entity/kinds/workspace.go`

```go
var WorkspaceKind = &KindSpec{
    Kind:        "Workspace",
    Singular:    "workspace",
    Plural:      "workspaces",
    Description: "User-owned container for views and queries",
    Namespaced:  true,
    Owned:       true,
    Hierarchical: false,
    SpecSchema:  workspaceSpecSchema,
}

// WorkspaceSpec
type WorkspaceSpec struct {
    DisplayName          string            `json:"display_name"`
    Description          string            `json:"description,omitempty"`
    Icon                 string            `json:"icon,omitempty"`
    DefaultTimeRange     *TimeRange        `json:"default_time_range,omitempty"`
    DefaultRefreshInterval int             `json:"default_refresh_interval,omitempty"`
    Settings             map[string]any    `json:"settings,omitempty"`
}
```

#### 2.2.2 View Kind

**Datei:** `internal/entity/kinds/view.go`

```go
var ViewKind = &KindSpec{
    Kind:        "View",
    Singular:    "view",
    Plural:      "views",
    Description: "Visualization of entity data",
    Namespaced:  true,
    Owned:       true,
    Hierarchical: true,  // Kann in Folder sein
    SpecSchema:  viewSpecSchema,
}

// ViewSpec - Vollständige View-Konfiguration
type ViewSpec struct {
    DisplayName string          `json:"display_name"`
    Description string          `json:"description,omitempty"`
    Icon        string          `json:"icon,omitempty"`
    Type        ViewType        `json:"type"`  // tree, table, board, stat, graph

    // Data
    Query       *QuerySpec      `json:"query"`
    Variables   []VariableSpec  `json:"variables,omitempty"`

    // Layout (type-specific)
    Hierarchy   *HierarchySpec  `json:"hierarchy,omitempty"`   // für tree
    Columns     []ColumnSpec    `json:"columns,omitempty"`     // für tree/table
    Cards       *CardSpec       `json:"cards,omitempty"`       // für board

    // Display
    Display     DisplaySpec     `json:"display"`

    // Interactions
    Interactions *InteractionSpec `json:"interactions,omitempty"`

    // Refresh
    Refresh     RefreshSpec     `json:"refresh"`
}
```

#### 2.2.3 Folder Kind

**Datei:** `internal/entity/kinds/folder.go`

```go
var FolderKind = &KindSpec{
    Kind:        "Folder",
    Singular:    "folder",
    Plural:      "folders",
    Description: "Container for organizing views",
    Namespaced:  true,
    Owned:       true,
    Hierarchical: true,
    SpecSchema:  folderSpecSchema,
}

// FolderSpec
type FolderSpec struct {
    DisplayName string       `json:"display_name"`
    Description string       `json:"description,omitempty"`
    Icon        string       `json:"icon,omitempty"`
    Color       string       `json:"color,omitempty"`
    Expanded    bool         `json:"expanded,omitempty"`

    // Dynamic folder
    Type        FolderType   `json:"type,omitempty"`  // static, dynamic
    ChildrenQuery string     `json:"children_query,omitempty"`
    ChildTemplate *FolderChildTemplate `json:"child_template,omitempty"`
}
```

#### 2.2.4 Query Kind

**Datei:** `internal/entity/kinds/query.go`

```go
var QueryKind = &KindSpec{
    Kind:        "Query",
    Singular:    "query",
    Plural:      "queries",
    Description: "Saved query for reuse",
    Namespaced:  true,
    Owned:       true,
    Hierarchical: false,
    SpecSchema:  querySpecSchema,
}

// QuerySpec
type QuerySpec struct {
    DisplayName string      `json:"display_name"`
    Description string      `json:"description,omitempty"`
    EntityType  string      `json:"entity_type"`  // Target, Poller, etc.
    Query       string      `json:"query"`        // UQL Query
    CacheTTL    int         `json:"cache_ttl,omitempty"`
}
```

**Tasks:**
- [ ] 2.2.1 Workspace Kind implementieren
- [ ] 2.2.2 View Kind implementieren (alle Sub-Specs)
- [ ] 2.2.3 Folder Kind implementieren
- [ ] 2.2.4 Query Kind implementieren
- [ ] 2.2.5 Variable Kind implementieren
- [ ] 2.2.6 JSON Schemas für alle Kinds erstellen

### 2.3 View Sub-Specifications

**Status:** [ ] TODO

**Datei:** `internal/entity/kinds/view_specs.go`

```go
// QuerySpec für Data Source
type QuerySpec struct {
    Base          string                 `json:"base"`           // UQL Base Query
    Variables     map[string]VariableDef `json:"variables,omitempty"`
    DynamicFilters []DynamicFilter       `json:"dynamic_filters,omitempty"`
    Ref           string                 `json:"ref,omitempty"`  // Reference to saved Query
    Extend        string                 `json:"extend,omitempty"`
}

// HierarchySpec für Tree Views
type HierarchySpec struct {
    Levels            []HierarchyLevel   `json:"levels"`
    StatusPropagation *StatusPropagation `json:"status_propagation,omitempty"`
}

type HierarchyLevel struct {
    ID               string         `json:"id"`
    GroupBy          string         `json:"group_by,omitempty"`
    IsLeaf           bool           `json:"is_leaf,omitempty"`
    Display          LevelDisplay   `json:"display"`
    Sort             string         `json:"sort,omitempty"`
    CollapsedDefault bool           `json:"collapsed_by_default,omitempty"`
}

// ColumnSpec für Spalten
type ColumnSpec struct {
    ID        string          `json:"id"`
    Field     string          `json:"field,omitempty"`
    Type      ColumnType      `json:"type,omitempty"`      // field, calculated, metric, relation_count
    Label     string          `json:"label,omitempty"`
    Width     interface{}     `json:"width,omitempty"`     // int oder "auto"
    Renderer  string          `json:"renderer,omitempty"`  // text, badge, percent_bar, sparkline, etc.
    Sortable  bool            `json:"sortable,omitempty"`
    Sort      string          `json:"sort,omitempty"`      // asc, desc

    // For calculated columns
    Formula   string          `json:"formula,omitempty"`

    // For metric columns
    Metric    string          `json:"metric,omitempty"`
    TimeRange string          `json:"time_range,omitempty"`

    // For relation_count
    Relation  string          `json:"relation,omitempty"`
    Filter    string          `json:"filter,omitempty"`

    // Formatting
    Format    string          `json:"format,omitempty"`
    Thresholds []Threshold    `json:"thresholds,omitempty"`

    // Actions
    ClickAction *ClickAction  `json:"click_action,omitempty"`
}

// DisplaySpec für Anzeige-Einstellungen
type DisplaySpec struct {
    Density         string   `json:"density,omitempty"`         // compact, comfortable, spacious
    RowHeight       int      `json:"row_height,omitempty"`
    AlternatingRows bool     `json:"alternating_rows,omitempty"`
    GridLines       string   `json:"grid_lines,omitempty"`      // none, horizontal, vertical, both

    Header *HeaderSpec      `json:"header,omitempty"`
    Tree   *TreeDisplaySpec `json:"tree,omitempty"`
    Empty  *EmptyStateSpec  `json:"empty_state,omitempty"`
}

// InteractionSpec für User-Interaktionen
type InteractionSpec struct {
    RowClick       *ActionSpec   `json:"row_click,omitempty"`
    RowDoubleClick *ActionSpec   `json:"row_double_click,omitempty"`
    ContextMenu    []MenuItem    `json:"context_menu,omitempty"`
    BulkActions    []BulkAction  `json:"bulk_actions,omitempty"`
    DragDrop       *DragDropSpec `json:"drag_drop,omitempty"`
}

// RefreshSpec für Aktualisierung
type RefreshSpec struct {
    Mode              string   `json:"mode"`               // manual, interval, realtime
    Interval          string   `json:"interval,omitempty"` // "30s", "5m"
    Realtime          *RealtimeSpec `json:"realtime,omitempty"`
}
```

**Tasks:**
- [ ] 2.3.1 Alle Sub-Specs implementieren
- [ ] 2.3.2 Validation für jeden Spec-Typ
- [ ] 2.3.3 Default-Werte definieren
- [ ] 2.3.4 Type-Aliase und Enums definieren

---

## Phase 3: Query Language (UQL)

**Ziel:** SQL-ähnliche Query Language für Entities

### 3.1 UQL Parser

**Status:** [ ] TODO

**Datei:** `internal/uql/parser.go`

```go
// UQL Grammar (vereinfacht):
//
// query      = "FROM" kind [where] [joins] [groupby] [orderby] [limit]
// where      = "WHERE" expression
// expression = comparison | logical | exists | in
// comparison = field operator value
// logical    = expression ("AND" | "OR") expression
// field      = identifier ("." identifier)*
// operator   = "=" | "!=" | ">" | ">=" | "<" | "<=" | "LIKE" | "CONTAINS"

type Parser struct {
    lexer *Lexer
    // ...
}

type Query struct {
    From      string
    Where     Expression
    Joins     []Join
    GroupBy   []string
    OrderBy   []OrderSpec
    Limit     int
    Offset    int
    Variables map[string]string  // Variable substitutions
}

type Expression interface {
    Evaluate(ctx *EvalContext, entity *Entity) (bool, error)
    ToSQL() (string, []interface{})
}

func Parse(input string) (*Query, error)
func (q *Query) Bind(vars map[string]interface{}) (*Query, error)
func (q *Query) ToSQL() (string, []interface{}, error)
```

**Tasks:**
- [ ] 3.1.1 Lexer implementieren
- [ ] 3.1.2 Parser implementieren
- [ ] 3.1.3 AST-Typen definieren
- [ ] 3.1.4 Expression Evaluation implementieren
- [ ] 3.1.5 SQL-Compilation implementieren
- [ ] 3.1.6 Variable Substitution implementieren

### 3.2 UQL Executor

**Status:** [ ] TODO

**Datei:** `internal/uql/executor.go`

```go
type Executor struct {
    store     *store.EntityStore
    registry  *entity.Registry
    cache     *QueryCache
}

type ExecuteOptions struct {
    Variables   map[string]interface{}
    TimeRange   *TimeRange
    MaxResults  int
    UseCache    bool
    CacheTTL    time.Duration
}

type QueryResult struct {
    Entities []*store.Entity
    Total    int
    Cached   bool
    Duration time.Duration
}

func (e *Executor) Execute(ctx context.Context, query string, opts ExecuteOptions) (*QueryResult, error)
func (e *Executor) ExecuteParsed(ctx context.Context, q *Query, opts ExecuteOptions) (*QueryResult, error)
func (e *Executor) Explain(ctx context.Context, query string) (*QueryPlan, error)
```

**Tasks:**
- [ ] 3.2.1 Executor implementieren
- [ ] 3.2.2 Query Caching implementieren
- [ ] 3.2.3 Query Plan/Explain implementieren
- [ ] 3.2.4 Performance-Optimierungen

### 3.3 UQL Tests

**Status:** [ ] TODO

**Datei:** `internal/uql/parser_test.go`, `internal/uql/executor_test.go`

**Tasks:**
- [ ] 3.3.1 Parser Tests (alle Syntax-Varianten)
- [ ] 3.3.2 Executor Tests
- [ ] 3.3.3 Edge Cases
- [ ] 3.3.4 Performance Tests

---

## Phase 4: View Rendering Engine

**Ziel:** Views in darstellbare Daten transformieren

### 4.1 View Renderer Interface

**Status:** [ ] TODO

**Datei:** `internal/view/renderer.go`

```go
type Renderer struct {
    executor  *uql.Executor
    registry  *entity.Registry
    store     *store.EntityStore
}

type RenderOptions struct {
    Variables   map[string]interface{}
    TimeRange   *TimeRange
    Path        string    // Für Tree: welchen Subtree rendern
    Depth       int       // Wie tief expandieren
    Page        int
    PageSize    int
}

type RenderResult struct {
    ViewID      string
    ViewType    string
    Path        string
    Nodes       []*RenderNode
    Columns     []ColumnDef
    Pagination  *PaginationInfo
    Variables   []VariableValue
    RenderedAt  time.Time
}

type RenderNode struct {
    ID          string
    Path        string
    Name        string
    Type        string          // folder, entity, separator
    Icon        string
    Color       string
    HasChildren bool
    ChildCount  int
    Expanded    bool

    // Entity data (wenn Type == entity)
    Entity      *EntityData

    // Aggregated status
    Status      *StatusSummary

    // Column values
    Values      map[string]interface{}
}

func (r *Renderer) Render(ctx context.Context, viewID string, opts RenderOptions) (*RenderResult, error)
func (r *Renderer) RenderNode(ctx context.Context, viewID, nodePath string, opts RenderOptions) (*RenderResult, error)
```

**Tasks:**
- [ ] 4.1.1 Renderer Interface definieren
- [ ] 4.1.2 RenderResult Typen definieren
- [ ] 4.1.3 Basis-Renderer implementieren

### 4.2 View Type Renderers

**Status:** [ ] TODO

**Dateien:** `internal/view/render_*.go`

#### 4.2.1 Tree Renderer

**Datei:** `internal/view/render_tree.go`

```go
type TreeRenderer struct {
    *Renderer
}

func (r *TreeRenderer) Render(ctx context.Context, view *ViewSpec, opts RenderOptions) (*RenderResult, error)
func (r *TreeRenderer) buildHierarchy(entities []*Entity, levels []HierarchyLevel) []*RenderNode
func (r *TreeRenderer) propagateStatus(nodes []*RenderNode, config StatusPropagation)
func (r *TreeRenderer) applyGrouping(entities []*Entity, groupBy string) map[string][]*Entity
```

#### 4.2.2 Table Renderer

**Datei:** `internal/view/render_table.go`

```go
type TableRenderer struct {
    *Renderer
}

func (r *TableRenderer) Render(ctx context.Context, view *ViewSpec, opts RenderOptions) (*RenderResult, error)
func (r *TableRenderer) computeColumns(entities []*Entity, columns []ColumnSpec) []map[string]interface{}
```

#### 4.2.3 Board Renderer (Kanban)

**Datei:** `internal/view/render_board.go`

#### 4.2.4 Stat Renderer

**Datei:** `internal/view/render_stat.go`

**Tasks:**
- [ ] 4.2.1 Tree Renderer implementieren
- [ ] 4.2.2 Table Renderer implementieren
- [ ] 4.2.3 Board Renderer implementieren
- [ ] 4.2.4 Stat Renderer implementieren
- [ ] 4.2.5 Graph Renderer implementieren (future)

### 4.3 Column Value Computation

**Status:** [ ] TODO

**Datei:** `internal/view/columns.go`

```go
type ColumnComputer struct {
    executor *uql.Executor
    metrics  MetricsClient  // Interface zum Metrics-Backend
}

func (c *ColumnComputer) ComputeValue(ctx context.Context, col ColumnSpec, entity *Entity) (interface{}, error)
func (c *ColumnComputer) ComputeFieldValue(entity *Entity, field string) interface{}
func (c *ColumnComputer) ComputeCalculatedValue(entity *Entity, formula string) (interface{}, error)
func (c *ColumnComputer) ComputeMetricValue(ctx context.Context, entity *Entity, metric string, timeRange string) (interface{}, error)
func (c *ColumnComputer) ComputeRelationCount(ctx context.Context, entity *Entity, relation, filter string) (int, error)
```

**Tasks:**
- [ ] 4.3.1 Field Value Extraction
- [ ] 4.3.2 Formula Parser & Evaluator
- [ ] 4.3.3 Metric Integration
- [ ] 4.3.4 Relation Count Computation

### 4.4 Renderer Tests

**Status:** [ ] TODO

**Tasks:**
- [ ] 4.4.1 Tree Renderer Tests
- [ ] 4.4.2 Table Renderer Tests
- [ ] 4.4.3 Column Computation Tests
- [ ] 4.4.4 Performance Tests

---

## Phase 5: REST API

**Ziel:** Vollständige HTTP API für das View System

### 5.1 API Router Setup

**Status:** [ ] TODO

**Datei:** `internal/api/router.go`

```go
func SetupRoutes(r chi.Router, deps *Dependencies) {
    // Workspaces
    r.Route("/api/v2/workspaces", func(r chi.Router) {
        r.Get("/", h.ListWorkspaces)
        r.Post("/", h.CreateWorkspace)
        r.Get("/{id}", h.GetWorkspace)
        r.Put("/{id}", h.UpdateWorkspace)
        r.Delete("/{id}", h.DeleteWorkspace)
        r.Post("/{id}/clone", h.CloneWorkspace)

        // Nested resources
        r.Get("/{id}/views", h.ListWorkspaceViews)
        r.Get("/{id}/folders", h.ListWorkspaceFolders)
        r.Get("/{id}/queries", h.ListWorkspaceQueries)
    })

    // Views
    r.Route("/api/v2/views", func(r chi.Router) {
        r.Post("/", h.CreateView)
        r.Get("/{id}", h.GetView)
        r.Put("/{id}", h.UpdateView)
        r.Patch("/{id}", h.PatchView)  // Partial update
        r.Delete("/{id}", h.DeleteView)

        // Rendering
        r.Get("/{id}/render", h.RenderView)
        r.Get("/{id}/render/{path:*}", h.RenderViewNode)

        // View nodes (für Tree-Strukturen innerhalb eines Views)
        r.Get("/{id}/nodes", h.ListViewNodes)
        r.Post("/{id}/nodes", h.CreateViewNode)
        r.Put("/{id}/nodes/{path:*}", h.UpdateViewNode)
        r.Delete("/{id}/nodes/{path:*}", h.DeleteViewNode)
    })

    // Folders
    r.Route("/api/v2/folders", func(r chi.Router) {
        r.Post("/", h.CreateFolder)
        r.Get("/{id}", h.GetFolder)
        r.Put("/{id}", h.UpdateFolder)
        r.Delete("/{id}", h.DeleteFolder)
        r.Get("/{id}/children", h.ListFolderChildren)
        r.Post("/{id}/move", h.MoveFolder)
    })

    // Queries
    r.Route("/api/v2/queries", func(r chi.Router) {
        r.Post("/", h.CreateQuery)
        r.Get("/{id}", h.GetQuery)
        r.Put("/{id}", h.UpdateQuery)
        r.Delete("/{id}", h.DeleteQuery)
        r.Post("/{id}/execute", h.ExecuteQuery)
        r.Post("/execute", h.ExecuteAdhocQuery)
    })

    // Generic entity API
    r.Route("/api/v2/entities", func(r chi.Router) {
        r.Get("/", h.ListEntities)
        r.Post("/", h.CreateEntity)
        r.Get("/{kind}/{namespace}/{name}", h.GetEntity)
        r.Put("/{kind}/{namespace}/{name}", h.UpdateEntity)
        r.Delete("/{kind}/{namespace}/{name}", h.DeleteEntity)
    })
}
```

**Tasks:**
- [ ] 5.1.1 Router Setup
- [ ] 5.1.2 Middleware (Auth, Logging, CORS)
- [ ] 5.1.3 Error Handling

### 5.2 API Handlers

**Status:** [ ] TODO

**Dateien:** `internal/api/handler_*.go`

#### 5.2.1 Workspace Handlers

**Datei:** `internal/api/handler_workspace.go`

**Tasks:**
- [ ] 5.2.1.1 ListWorkspaces
- [ ] 5.2.1.2 CreateWorkspace
- [ ] 5.2.1.3 GetWorkspace
- [ ] 5.2.1.4 UpdateWorkspace
- [ ] 5.2.1.5 DeleteWorkspace
- [ ] 5.2.1.6 CloneWorkspace

#### 5.2.2 View Handlers

**Datei:** `internal/api/handler_view.go`

**Tasks:**
- [ ] 5.2.2.1 CreateView
- [ ] 5.2.2.2 GetView
- [ ] 5.2.2.3 UpdateView
- [ ] 5.2.2.4 PatchView (partial updates - wichtig für Live-Editing!)
- [ ] 5.2.2.5 DeleteView
- [ ] 5.2.2.6 RenderView
- [ ] 5.2.2.7 RenderViewNode

#### 5.2.3 Folder Handlers

**Datei:** `internal/api/handler_folder.go`

**Tasks:**
- [ ] 5.2.3.1 CRUD Handlers
- [ ] 5.2.3.2 ListChildren
- [ ] 5.2.3.3 MoveFolder

#### 5.2.4 Query Handlers

**Datei:** `internal/api/handler_query.go`

**Tasks:**
- [ ] 5.2.4.1 CRUD Handlers
- [ ] 5.2.4.2 ExecuteQuery
- [ ] 5.2.4.3 ExecuteAdhocQuery

### 5.3 Request/Response Types

**Status:** [ ] TODO

**Datei:** `internal/api/types.go`

```go
// Workspace
type CreateWorkspaceRequest struct {
    Namespace   string         `json:"namespace"`
    Name        string         `json:"name"`
    DisplayName string         `json:"display_name"`
    Description string         `json:"description,omitempty"`
    Settings    map[string]any `json:"settings,omitempty"`
}

// View
type CreateViewRequest struct {
    WorkspaceID string          `json:"workspace_id"`
    ParentID    *string         `json:"parent_id,omitempty"`
    Name        string          `json:"name"`
    Spec        json.RawMessage `json:"spec"`
}

type PatchViewRequest struct {
    // JSON Patch operations
    Operations []PatchOperation `json:"operations"`
}

type PatchOperation struct {
    Op    string      `json:"op"`    // add, remove, replace, move
    Path  string      `json:"path"`  // JSON Pointer path
    Value interface{} `json:"value,omitempty"`
}

type RenderViewRequest struct {
    Variables map[string]interface{} `json:"variables,omitempty"`
    TimeRange *TimeRange             `json:"time_range,omitempty"`
    Path      string                 `json:"path,omitempty"`
    Depth     int                    `json:"depth,omitempty"`
    Page      int                    `json:"page,omitempty"`
    PageSize  int                    `json:"page_size,omitempty"`
}

type RenderViewResponse struct {
    ViewID     string               `json:"view_id"`
    ViewType   string               `json:"view_type"`
    Path       string               `json:"path"`
    Nodes      []*RenderNode        `json:"nodes"`
    Columns    []ColumnDef          `json:"columns,omitempty"`
    Pagination *PaginationInfo      `json:"pagination,omitempty"`
    Variables  []VariableValue      `json:"variables,omitempty"`
    RenderedAt time.Time            `json:"rendered_at"`
}
```

**Tasks:**
- [ ] 5.3.1 Request Types definieren
- [ ] 5.3.2 Response Types definieren
- [ ] 5.3.3 Validation implementieren
- [ ] 5.3.4 JSON Patch Support

### 5.4 API Tests

**Status:** [ ] TODO

**Tasks:**
- [ ] 5.4.1 Unit Tests für alle Handlers
- [ ] 5.4.2 Integration Tests
- [ ] 5.4.3 API Documentation (OpenAPI)

---

## Phase 6: Real-time Subscriptions

**Ziel:** WebSocket-basierte Live-Updates

### 6.1 Subscription Manager

**Status:** [ ] TODO

**Datei:** `internal/realtime/manager.go`

```go
type SubscriptionManager struct {
    store       *store.EntityStore
    renderer    *view.Renderer
    hub         *Hub
    subscribers map[string]*Subscriber  // sessionID -> Subscriber
}

type Subscriber struct {
    SessionID     string
    UserID        string
    Conn          *websocket.Conn
    Subscriptions []*Subscription
    Send          chan []byte
}

type Subscription struct {
    ID        string
    ViewID    string
    NodePath  string
    Mode      string  // full, diff, count_only
    Throttle  time.Duration
    LastPush  time.Time
}

func (m *SubscriptionManager) Subscribe(ctx context.Context, sub *Subscription) error
func (m *SubscriptionManager) Unsubscribe(ctx context.Context, subID string) error
func (m *SubscriptionManager) NotifyChange(ctx context.Context, change EntityChange)
```

**Tasks:**
- [ ] 6.1.1 Subscription Manager implementieren
- [ ] 6.1.2 WebSocket Hub implementieren
- [ ] 6.1.3 Change Detection implementieren

### 6.2 WebSocket Handler

**Status:** [ ] TODO

**Datei:** `internal/realtime/websocket.go`

```go
// WebSocket Message Types
type WSMessage struct {
    Type    string          `json:"type"`
    ID      string          `json:"id,omitempty"`
    Payload json.RawMessage `json:"payload"`
}

// Message Types:
// - subscribe: Subscribe to view updates
// - unsubscribe: Unsubscribe
// - update: Server pushes update
// - ping/pong: Keep-alive

type SubscribePayload struct {
    ViewID   string `json:"view_id"`
    NodePath string `json:"node_path,omitempty"`
    Mode     string `json:"mode"`  // full, diff
}

type UpdatePayload struct {
    ViewID    string          `json:"view_id"`
    NodePath  string          `json:"node_path,omitempty"`
    Type      string          `json:"type"`  // full, diff, remove
    Data      json.RawMessage `json:"data"`
    Timestamp time.Time       `json:"timestamp"`
    Seq       uint64          `json:"seq"`
}

func (h *WSHandler) HandleConnection(w http.ResponseWriter, r *http.Request)
func (h *WSHandler) readPump(subscriber *Subscriber)
func (h *WSHandler) writePump(subscriber *Subscriber)
```

**Tasks:**
- [ ] 6.2.1 WebSocket Upgrade Handler
- [ ] 6.2.2 Read/Write Pumps
- [ ] 6.2.3 Message Routing
- [ ] 6.2.4 Heartbeat/Ping-Pong

### 6.3 Change Propagation

**Status:** [ ] TODO

**Datei:** `internal/realtime/changes.go`

```go
type EntityChange struct {
    Type      string     // create, update, delete
    Entity    *Entity
    OldEntity *Entity    // für updates
    Fields    []string   // welche Felder geändert
}

type ChangeProcessor struct {
    manager *SubscriptionManager
}

func (p *ChangeProcessor) Process(ctx context.Context, change EntityChange)
func (p *ChangeProcessor) computeDiff(old, new *RenderNode) *DiffResult
func (p *ChangeProcessor) shouldNotify(sub *Subscription, change EntityChange) bool
```

**Tasks:**
- [ ] 6.3.1 Change Detection
- [ ] 6.3.2 Diff Computation
- [ ] 6.3.3 Throttling
- [ ] 6.3.4 Batch Updates

### 6.4 SSE Alternative

**Status:** [ ] TODO

**Datei:** `internal/realtime/sse.go`

```go
// Server-Sent Events als Alternative zu WebSocket
func (h *SSEHandler) HandleStream(w http.ResponseWriter, r *http.Request)
```

**Tasks:**
- [ ] 6.4.1 SSE Handler implementieren
- [ ] 6.4.2 Reconnection Support

---

## Phase 7: Migration & Integration

**Ziel:** Integration mit bestehendem Stalker-Code

### 7.1 Migration von alten Tabellen

**Status:** [ ] TODO

**Datei:** `internal/store/migrations/004_migrate_to_entities.sql`

```sql
-- Migrate targets zu entities
INSERT INTO entities (kind, namespace, name, labels, spec, status, source, created_at)
SELECT
    'Target',
    namespace,
    name,
    labels,
    jsonb_build_object(
        'host', host,
        'description', description,
        'config', config
    ),
    jsonb_build_object(
        'health', 'unknown'
    ),
    source,
    created_at
FROM targets;

-- Migrate pollers zu entities
INSERT INTO entities (kind, namespace, name, labels, spec, status, source, created_at)
SELECT
    'Poller',
    namespace,
    name,
    labels,
    jsonb_build_object(
        'target', target,
        'oid', oid,
        'interval_ms', interval_ms,
        'config', config
    ),
    jsonb_build_object(
        'admin_state', admin_state,
        'oper_state', oper_state
    ),
    source,
    created_at
FROM pollers;

-- Relations für Target -> Poller
INSERT INTO relations (from_kind, from_namespace, from_name, to_kind, to_namespace, to_name, relation_type)
SELECT
    'Target', namespace, target,
    'Poller', namespace, name,
    'has_poller'
FROM pollers;
```

**Tasks:**
- [ ] 7.1.1 Migration Script erstellen
- [ ] 7.1.2 Rollback Script
- [ ] 7.1.3 Data Validation

### 7.2 Manager Integration

**Status:** [ ] TODO

**Datei:** `internal/manager/entity_manager.go`

```go
// EntityManager als Wrapper um EntityStore
type EntityManager struct {
    store    *store.EntityStore
    registry *entity.Registry

    // Legacy compatibility
    targets  *TargetManager
    pollers  *PollerManager
}

// Methoden die sowohl über alte als auch neue API funktionieren
func (m *EntityManager) GetTarget(namespace, name string) (*Target, error)
func (m *EntityManager) GetTargetAsEntity(namespace, name string) (*Entity, error)
```

**Tasks:**
- [ ] 7.2.1 EntityManager implementieren
- [ ] 7.2.2 Legacy Compatibility Layer
- [ ] 7.2.3 Gradual Migration Path

### 7.3 Handler Integration

**Status:** [ ] TODO

**Datei:** `internal/handler/entity_handler.go`

```go
type EntityHandler struct {
    *Handler
    entityMgr *EntityManager
    renderer  *view.Renderer
}

// Neue Handler-Methoden
func (h *EntityHandler) ListEntities(ctx *RequestContext, req *ListEntitiesRequest) (*ListEntitiesResponse, error)
func (h *EntityHandler) GetEntity(ctx *RequestContext, req *GetEntityRequest) (*GetEntityResponse, error)
func (h *EntityHandler) RenderView(ctx *RequestContext, req *RenderViewRequest) (*RenderViewResponse, error)
```

**Tasks:**
- [ ] 7.3.1 EntityHandler implementieren
- [ ] 7.3.2 ViewHandler implementieren
- [ ] 7.3.3 Wire Protocol Messages hinzufügen

### 7.4 Proto Updates

**Status:** [ ] TODO

**Datei:** `proto/stalker.proto`

```protobuf
// Neue Messages für Entity System
message Entity {
    string id = 1;
    string kind = 2;
    string namespace = 3;
    string name = 4;
    string owner = 5;
    map<string, string> labels = 6;
    google.protobuf.Struct spec = 7;
    google.protobuf.Struct status = 8;
    int32 version = 9;
    google.protobuf.Timestamp created_at = 10;
    google.protobuf.Timestamp updated_at = 11;
}

message ListEntitiesRequest {
    string kind = 1;
    string namespace = 2;
    string owner = 3;
    map<string, string> labels = 4;
    int32 limit = 5;
    int32 offset = 6;
}

message ListEntitiesResponse {
    repeated Entity entities = 1;
    int32 total = 2;
}

message RenderViewRequest {
    string view_id = 1;
    string path = 2;
    map<string, string> variables = 3;
    int32 depth = 4;
}

message RenderViewResponse {
    string view_id = 1;
    string view_type = 2;
    repeated RenderNode nodes = 3;
    repeated ColumnDef columns = 4;
}

message RenderNode {
    string id = 1;
    string path = 2;
    string name = 3;
    string type = 4;
    string icon = 5;
    bool has_children = 6;
    int32 child_count = 7;
    map<string, google.protobuf.Value> values = 8;
}
```

**Tasks:**
- [ ] 7.4.1 Proto Messages definieren
- [ ] 7.4.2 Proto generieren
- [ ] 7.4.3 Wire Handler updaten

---

## Dateistruktur (Neu)

```
internal/
├── store/
│   ├── entity.go              # Entity types
│   ├── entity_ops.go          # CRUD operations
│   ├── entity_query.go        # Query building
│   └── migrations/
│       ├── 003_entities.sql
│       └── 004_migrate_to_entities.sql
│
├── entity/
│   ├── registry.go            # Kind registry
│   ├── validation.go          # Schema validation
│   └── kinds/
│       ├── workspace.go
│       ├── view.go
│       ├── view_specs.go
│       ├── folder.go
│       └── query.go
│
├── uql/
│   ├── lexer.go
│   ├── parser.go
│   ├── ast.go
│   ├── executor.go
│   ├── cache.go
│   └── sql_compiler.go
│
├── view/
│   ├── renderer.go
│   ├── render_tree.go
│   ├── render_table.go
│   ├── render_board.go
│   ├── render_stat.go
│   └── columns.go
│
├── realtime/
│   ├── manager.go
│   ├── websocket.go
│   ├── sse.go
│   └── changes.go
│
├── api/
│   ├── router.go
│   ├── middleware.go
│   ├── handler_workspace.go
│   ├── handler_view.go
│   ├── handler_folder.go
│   ├── handler_query.go
│   ├── handler_entity.go
│   └── types.go
│
└── manager/
    └── entity_manager.go      # Integration layer
```

---

## Testplan

### Unit Tests
- [ ] Store: Entity CRUD
- [ ] Store: Relations
- [ ] UQL: Parser
- [ ] UQL: Executor
- [ ] View: All Renderers
- [ ] API: All Handlers

### Integration Tests
- [ ] Full workflow: Create workspace → Create view → Render
- [ ] Real-time: Subscribe → Change → Receive update
- [ ] Migration: Old data → New schema

### Performance Tests
- [ ] 10,000 Entities
- [ ] 1,000 concurrent renders
- [ ] 100 WebSocket subscribers

---

## Offene Fragen

1. **DuckDB vs PostgreSQL**: UQL muss für beide DBs funktionieren
2. **Metrics Integration**: Wie werden Metriken abgefragt?
3. **Alerting Integration**: Wie werden Alerts in Views eingebunden?
4. **Multi-Tenancy**: Namespace-übergreifende Views?

---

## Changelog

| Datum | Version | Änderung |
|-------|---------|----------|
| 2025-01-17 | 1.0 | Initial plan created |

---

## Nächste Schritte

1. **Review dieses Plans** - Feedback sammeln
2. **Phase 1 starten** - Entity Store Foundation
3. **Iterativ entwickeln** - Eine Phase nach der anderen

---

*Dieses Dokument wird während der Implementierung aktualisiert.*
