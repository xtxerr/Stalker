package sync

import (
	"encoding/json"

	"github.com/xtxerr/stalker/internal/store"
)

// =============================================================================
// Namespace Adapter
// =============================================================================

// SyncableNamespace wraps store.Namespace to implement Syncable.
type SyncableNamespace struct {
	*store.Namespace
}

// WrapNamespace wraps a namespace for syncing.
func WrapNamespace(ns *store.Namespace) *SyncableNamespace {
	return &SyncableNamespace{Namespace: ns}
}

// WrapNamespaces wraps multiple namespaces for syncing.
func WrapNamespaces(namespaces []*store.Namespace) []*SyncableNamespace {
	result := make([]*SyncableNamespace, len(namespaces))
	for i, ns := range namespaces {
		result[i] = WrapNamespace(ns)
	}
	return result
}

// UnwrapNamespace extracts the underlying namespace.
func (s *SyncableNamespace) UnwrapNamespace() *store.Namespace {
	return s.Namespace
}

// SyncKey implements Syncable.
func (s *SyncableNamespace) SyncKey() string {
	return s.Name
}

// SyncHash implements Syncable.
func (s *SyncableNamespace) SyncHash() uint64 {
	b := NewHashBuilder().
		String(s.Name).
		String(s.Description)

	// Hash config if present
	if s.Config != nil {
		configJSON, _ := json.Marshal(s.Config)
		b.Bytes(configJSON)
	}

	return b.Build()
}

// SyncSource implements Syncable.
func (s *SyncableNamespace) SyncSource() Source {
	if s.Namespace.Source == "" {
		return SourceAPI
	}
	return Source(s.Namespace.Source)
}

// SetSyncSource implements Syncable.
func (s *SyncableNamespace) SetSyncSource(src Source) {
	s.Namespace.Source = string(src)
}

// SyncDependencies implements Syncable.
func (s *SyncableNamespace) SyncDependencies() []string {
	return nil // Namespaces have no dependencies
}

// =============================================================================
// Target Adapter
// =============================================================================

// SyncableTarget wraps store.Target to implement Syncable.
type SyncableTarget struct {
	*store.Target
}

// WrapTarget wraps a target for syncing.
func WrapTarget(t *store.Target) *SyncableTarget {
	return &SyncableTarget{Target: t}
}

// WrapTargets wraps multiple targets for syncing.
func WrapTargets(targets []*store.Target) []*SyncableTarget {
	result := make([]*SyncableTarget, len(targets))
	for i, t := range targets {
		result[i] = WrapTarget(t)
	}
	return result
}

// UnwrapTarget extracts the underlying target.
func (s *SyncableTarget) UnwrapTarget() *store.Target {
	return s.Target
}

// SyncKey implements Syncable.
func (s *SyncableTarget) SyncKey() string {
	return s.Namespace + "/" + s.Name
}

// SyncHash implements Syncable.
func (s *SyncableTarget) SyncHash() uint64 {
	b := NewHashBuilder().
		String(s.Namespace).
		String(s.Name).
		String(s.Description).
		StringMap(s.Labels)

	// Hash config if present
	if s.Config != nil {
		configJSON, _ := json.Marshal(s.Config)
		b.Bytes(configJSON)
	}

	return b.Build()
}

// SyncSource implements Syncable.
func (s *SyncableTarget) SyncSource() Source {
	if s.Target.Source == "" {
		return SourceAPI
	}
	return Source(s.Target.Source)
}

// SetSyncSource implements Syncable.
func (s *SyncableTarget) SetSyncSource(src Source) {
	s.Target.Source = string(src)
}

// SyncDependencies implements Syncable.
func (s *SyncableTarget) SyncDependencies() []string {
	return []string{s.Namespace} // Target depends on its namespace
}

// =============================================================================
// Poller Adapter
// =============================================================================

// SyncablePoller wraps store.Poller to implement Syncable.
type SyncablePoller struct {
	*store.Poller
}

// WrapPoller wraps a poller for syncing.
func WrapPoller(p *store.Poller) *SyncablePoller {
	return &SyncablePoller{Poller: p}
}

// WrapPollers wraps multiple pollers for syncing.
func WrapPollers(pollers []*store.Poller) []*SyncablePoller {
	result := make([]*SyncablePoller, len(pollers))
	for i, p := range pollers {
		result[i] = WrapPoller(p)
	}
	return result
}

// UnwrapPoller extracts the underlying poller.
func (s *SyncablePoller) UnwrapPoller() *store.Poller {
	return s.Poller
}

// SyncKey implements Syncable.
func (s *SyncablePoller) SyncKey() string {
	return s.Namespace + "/" + s.Target + "/" + s.Name
}

// SyncHash implements Syncable.
func (s *SyncablePoller) SyncHash() uint64 {
	b := NewHashBuilder().
		String(s.Namespace).
		String(s.Target).
		String(s.Name).
		String(s.Description).
		String(s.Protocol).
		Bytes(s.ProtocolConfig).
		String(s.AdminState)

	// Hash polling config if present
	if s.PollingConfig != nil {
		configJSON, _ := json.Marshal(s.PollingConfig)
		b.Bytes(configJSON)
	}

	return b.Build()
}

// SyncSource implements Syncable.
func (s *SyncablePoller) SyncSource() Source {
	if s.Poller.Source == "" {
		return SourceAPI
	}
	return Source(s.Poller.Source)
}

// SetSyncSource implements Syncable.
func (s *SyncablePoller) SetSyncSource(src Source) {
	s.Poller.Source = string(src)
}

// SyncDependencies implements Syncable.
func (s *SyncablePoller) SyncDependencies() []string {
	// Poller depends on its target (and transitively, its namespace)
	return []string{s.Namespace + "/" + s.Target}
}

// =============================================================================
// Tree Node Adapter
// =============================================================================

// SyncableTreeNode wraps store.TreeNode to implement Syncable.
type SyncableTreeNode struct {
	*store.TreeNode
}

// WrapTreeNode wraps a tree node for syncing.
func WrapTreeNode(n *store.TreeNode) *SyncableTreeNode {
	return &SyncableTreeNode{TreeNode: n}
}

// WrapTreeNodes wraps multiple tree nodes for syncing.
func WrapTreeNodes(nodes []*store.TreeNode) []*SyncableTreeNode {
	result := make([]*SyncableTreeNode, len(nodes))
	for i, n := range nodes {
		result[i] = WrapTreeNode(n)
	}
	return result
}

// UnwrapTreeNode extracts the underlying tree node.
func (s *SyncableTreeNode) UnwrapTreeNode() *store.TreeNode {
	return s.TreeNode
}

// SyncKey implements Syncable.
func (s *SyncableTreeNode) SyncKey() string {
	return s.Namespace + ":" + s.Path
}

// SyncHash implements Syncable.
func (s *SyncableTreeNode) SyncHash() uint64 {
	return NewHashBuilder().
		String(s.Namespace).
		String(s.Path).
		String(s.NodeType).
		String(s.LinkRef).
		String(s.Description).
		Build()
}

// SyncSource implements Syncable.
func (s *SyncableTreeNode) SyncSource() Source {
	if s.TreeNode.Source == "" {
		return SourceAPI
	}
	return Source(s.TreeNode.Source)
}

// SetSyncSource implements Syncable.
func (s *SyncableTreeNode) SetSyncSource(src Source) {
	s.TreeNode.Source = string(src)
}

// SyncDependencies implements Syncable.
func (s *SyncableTreeNode) SyncDependencies() []string {
	// Tree nodes depend on the entities they link to
	// For now, we process tree after all entities
	return nil
}

// =============================================================================
// Tree View Adapter
// =============================================================================

// TreeView represents a label-based view definition.
type TreeView struct {
	Namespace    string
	Name         string
	Description  string
	PathTemplate string
	EntityType   string // "target" or "poller"
	FilterExpr   string
	Priority     int
	Enabled      bool
	Source       string
}

// SyncableTreeView wraps TreeView to implement Syncable.
type SyncableTreeView struct {
	*TreeView
}

// WrapTreeView wraps a tree view for syncing.
func WrapTreeView(v *TreeView) *SyncableTreeView {
	return &SyncableTreeView{TreeView: v}
}

// SyncKey implements Syncable.
func (s *SyncableTreeView) SyncKey() string {
	return s.Namespace + "/view/" + s.Name
}

// SyncHash implements Syncable.
func (s *SyncableTreeView) SyncHash() uint64 {
	return NewHashBuilder().
		String(s.Namespace).
		String(s.Name).
		String(s.Description).
		String(s.PathTemplate).
		String(s.EntityType).
		String(s.FilterExpr).
		Int(s.Priority).
		Bool(s.Enabled).
		Build()
}

// SyncSource implements Syncable.
func (s *SyncableTreeView) SyncSource() Source {
	if s.TreeView.Source == "" {
		return SourceAPI
	}
	return Source(s.TreeView.Source)
}

// SetSyncSource implements Syncable.
func (s *SyncableTreeView) SetSyncSource(src Source) {
	s.TreeView.Source = string(src)
}

// SyncDependencies implements Syncable.
func (s *SyncableTreeView) SyncDependencies() []string {
	return []string{s.Namespace}
}

// =============================================================================
// Tree Smart Folder Adapter
// =============================================================================

// TreeSmartFolder represents a query-based smart folder.
type TreeSmartFolder struct {
	Namespace   string
	Path        string
	Description string
	TargetQuery string
	PollerQuery string
	CacheTTL    int // seconds
	Priority    int
	Enabled     bool
	Source      string
}

// SyncableSmartFolder wraps TreeSmartFolder to implement Syncable.
type SyncableSmartFolder struct {
	*TreeSmartFolder
}

// WrapSmartFolder wraps a smart folder for syncing.
func WrapSmartFolder(f *TreeSmartFolder) *SyncableSmartFolder {
	return &SyncableSmartFolder{TreeSmartFolder: f}
}

// SyncKey implements Syncable.
func (s *SyncableSmartFolder) SyncKey() string {
	return s.Namespace + "/smart:" + s.Path
}

// SyncHash implements Syncable.
func (s *SyncableSmartFolder) SyncHash() uint64 {
	return NewHashBuilder().
		String(s.Namespace).
		String(s.Path).
		String(s.Description).
		String(s.TargetQuery).
		String(s.PollerQuery).
		Int(s.CacheTTL).
		Int(s.Priority).
		Bool(s.Enabled).
		Build()
}

// SyncSource implements Syncable.
func (s *SyncableSmartFolder) SyncSource() Source {
	if s.TreeSmartFolder.Source == "" {
		return SourceAPI
	}
	return Source(s.TreeSmartFolder.Source)
}

// SetSyncSource implements Syncable.
func (s *SyncableSmartFolder) SetSyncSource(src Source) {
	s.TreeSmartFolder.Source = string(src)
}

// SyncDependencies implements Syncable.
func (s *SyncableSmartFolder) SyncDependencies() []string {
	return []string{s.Namespace}
}
