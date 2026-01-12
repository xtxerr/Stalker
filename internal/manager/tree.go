// Package manager provides business logic and entity management for stalker.
//
// This file contains tree-related operations.
//
package manager

import (
	"fmt"

	"github.com/xtxerr/stalker/internal/logging"
	"github.com/xtxerr/stalker/internal/store"
	"github.com/xtxerr/stalker/internal/validation"
)

var treeLog = logging.Component("manager.tree")

// =============================================================================
// Constants
// =============================================================================

const (
	LinkPrefixTarget = "target:"
	LinkPrefixPoller = "poller:"
)

// =============================================================================
// TreeManager
// =============================================================================

// TreeManager handles tree operations.
type TreeManager struct {
	store *store.Store
}

// NewTreeManager creates a new tree manager.
func NewTreeManager(s *store.Store) *TreeManager {
	return &TreeManager{store: s}
}

// =============================================================================
// =============================================================================

// ParseLinkRef parses a link reference from the database format.
//
func ParseLinkRef(ref string) (linkType string, target string, poller string, err error) {
	if ref == "" {
		return "", "", "", fmt.Errorf("empty link reference")
	}

	linkRef, err := validation.ParseLinkRef(ref)
	if err != nil {
		return "", "", "", err
	}

	return string(linkRef.Type), linkRef.Target, linkRef.Poller, nil
}

// =============================================================================
// LinkInfo - Struct-Based Alternative
// =============================================================================

// LinkInfo represents a parsed link reference.
type LinkInfo struct {
	Type   string
	Target string
	Poller string
}

// ParseLinkRefStruct parses a link reference and returns a struct.
func ParseLinkRefStruct(ref string) (*LinkInfo, error) {
	linkRef, err := validation.ParseLinkRef(ref)
	if err != nil {
		return nil, err
	}

	return &LinkInfo{
		Type:   string(linkRef.Type),
		Target: linkRef.Target,
		Poller: linkRef.Poller,
	}, nil
}

// IsTargetLink returns true if this is a target link.
func (l *LinkInfo) IsTargetLink() bool {
	return l.Type == "target"
}

// IsPollerLink returns true if this is a poller link.
func (l *LinkInfo) IsPollerLink() bool {
	return l.Type == "poller"
}

// String returns the database format of the link reference.
func (l *LinkInfo) String() string {
	if l.Type == "target" {
		return LinkPrefixTarget + l.Target
	}
	return fmt.Sprintf("%s%s/%s", LinkPrefixPoller, l.Target, l.Poller)
}

// =============================================================================
// =============================================================================

// BuildTargetLinkRef builds a target link reference string.
func BuildTargetLinkRef(targetName string) (string, error) {
	if targetName == "" {
		return "", fmt.Errorf("empty target name")
	}
	return LinkPrefixTarget + targetName, nil
}

// BuildPollerLinkRef builds a poller link reference string.
func BuildPollerLinkRef(targetName, pollerName string) (string, error) {
	if targetName == "" {
		return "", fmt.Errorf("empty target name")
	}
	if pollerName == "" {
		return "", fmt.Errorf("empty poller name")
	}
	return fmt.Sprintf("%s%s/%s", LinkPrefixPoller, targetName, pollerName), nil
}

// =============================================================================
// Tree Operations
// =============================================================================

// CreateLink creates a tree link.
func (m *TreeManager) CreateLink(namespace, treePath, linkName, linkType, linkRef string) error {
	return m.store.CreateTreeLink(namespace, treePath, linkName, linkType, linkRef)
}

// DeleteLink deletes a tree node.
func (m *TreeManager) DeleteLink(namespace, path string) error {
	return m.store.DeleteTreeNode(namespace, path)
}

// GetNode returns a tree node.
func (m *TreeManager) GetNode(namespace, path string) (*store.TreeNode, error) {
	return m.store.GetTreeNode(namespace, path)
}

// ListChildren returns children of a tree node.
func (m *TreeManager) ListChildren(namespace, parentPath string) ([]*store.TreeNode, error) {
	return m.store.ListTreeChildren(namespace, parentPath)
}

// DeleteLinksToTarget deletes all links pointing to a target.
func (m *TreeManager) DeleteLinksToTarget(namespace, targetName string) (int, error) {
	return m.store.DeleteLinksToTarget(namespace, targetName)
}

// DeleteLinksToPoller deletes all links pointing to a poller.
func (m *TreeManager) DeleteLinksToPoller(namespace, targetName, pollerName string) (int, error) {
	return m.store.DeleteLinksToPoller(namespace, targetName, pollerName)
}

// DeleteLinksToTargetAndPollers deletes all links to a target and its pollers.
func (m *TreeManager) DeleteLinksToTargetAndPollers(namespace, targetName string) (int, error) {
	return m.store.DeleteLinksToTargetAndPollers(namespace, targetName)
}

// GetLinksToTarget returns all paths linking to a target.
func (m *TreeManager) GetLinksToTarget(namespace, targetName string) ([]string, error) {
	return m.store.GetLinksToTarget(namespace, targetName)
}

// GetLinksToPoller returns all paths linking to a poller.
func (m *TreeManager) GetLinksToPoller(namespace, targetName, pollerName string) ([]string, error) {
	return m.store.GetLinksToPoller(namespace, targetName, pollerName)
}

// ResolveLink resolves a link to its target or poller.
func (m *TreeManager) ResolveLink(node *store.TreeNode) (*LinkInfo, error) {
	if node == nil || node.LinkRef == "" {
		return nil, fmt.Errorf("node has no link reference")
	}
	return ParseLinkRefStruct(node.LinkRef)
}
