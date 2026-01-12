package sync

import (
	"context"
	"testing"
)

// =============================================================================
// Policy Router Tests
// =============================================================================

func TestPolicyRouter_ExactMatch(t *testing.T) {
	r := NewPolicyRouter(PolicyCreateOnly)
	r.Set("entities.targets", PolicyFullSync)

	got := r.Get("entities.targets")
	if got != PolicyFullSync {
		t.Errorf("Get(entities.targets) = %v, want %v", got, PolicyFullSync)
	}
}

func TestPolicyRouter_HierarchicalLookup(t *testing.T) {
	r := NewPolicyRouter(PolicyCreateOnly)
	r.Set("tree", PolicyFullSync)

	// Should inherit from "tree"
	got := r.Get("tree.views")
	if got != PolicyFullSync {
		t.Errorf("Get(tree.views) = %v, want %v", got, PolicyFullSync)
	}

	// Deeper path should also inherit
	got = r.Get("tree.views.by-site")
	if got != PolicyFullSync {
		t.Errorf("Get(tree.views.by-site) = %v, want %v", got, PolicyFullSync)
	}
}

func TestPolicyRouter_SpecificOverridesGeneral(t *testing.T) {
	r := NewPolicyRouter(PolicyCreateOnly)
	r.Set("tree", PolicyFullSync)
	r.Set("tree.smart", PolicySourceAware)

	// "tree" should be FullSync
	got := r.Get("tree")
	if got != PolicyFullSync {
		t.Errorf("Get(tree) = %v, want %v", got, PolicyFullSync)
	}

	// "tree.views" inherits from "tree"
	got = r.Get("tree.views")
	if got != PolicyFullSync {
		t.Errorf("Get(tree.views) = %v, want %v", got, PolicyFullSync)
	}

	// "tree.smart" has specific override
	got = r.Get("tree.smart")
	if got != PolicySourceAware {
		t.Errorf("Get(tree.smart) = %v, want %v", got, PolicySourceAware)
	}
}

func TestPolicyRouter_DefaultFallback(t *testing.T) {
	r := NewPolicyRouter(PolicyCreateOnly)
	r.Set("entities", PolicyFullSync)

	// Unknown path should use default
	got := r.Get("unknown.path")
	if got != PolicyCreateOnly {
		t.Errorf("Get(unknown.path) = %v, want %v", got, PolicyCreateOnly)
	}
}

// =============================================================================
// Hash Builder Tests
// =============================================================================

func TestHashBuilder_Deterministic(t *testing.T) {
	h1 := NewHashBuilder().String("hello").Int(42).Build()
	h2 := NewHashBuilder().String("hello").Int(42).Build()

	if h1 != h2 {
		t.Errorf("Same inputs produced different hashes: %d != %d", h1, h2)
	}
}

func TestHashBuilder_DifferentOrder(t *testing.T) {
	h1 := NewHashBuilder().String("a").String("b").Build()
	h2 := NewHashBuilder().String("b").String("a").Build()

	if h1 == h2 {
		t.Error("Different order should produce different hashes")
	}
}

func TestHashBuilder_StringMap(t *testing.T) {
	m1 := map[string]string{"a": "1", "b": "2"}
	m2 := map[string]string{"b": "2", "a": "1"} // Same content, different order

	h1 := NewHashBuilder().StringMap(m1).Build()
	h2 := NewHashBuilder().StringMap(m2).Build()

	if h1 != h2 {
		t.Errorf("Same map content should produce same hash: %d != %d", h1, h2)
	}
}

func TestHashBuilder_NilSafe(t *testing.T) {
	// Should not panic
	h := NewHashBuilder().
		StringMap(nil).
		OptionalString(nil).
		OptionalUint32(nil).
		Build()

	if h == 0 {
		t.Error("Hash should be non-zero even with nil values")
	}
}

// =============================================================================
// Diff Engine Tests
// =============================================================================

// mockSyncable implements Syncable for testing.
type mockSyncable struct {
	key    string
	hash   uint64
	source Source
}

func (m *mockSyncable) SyncKey() string         { return m.key }
func (m *mockSyncable) SyncHash() uint64        { return m.hash }
func (m *mockSyncable) SyncSource() Source      { return m.source }
func (m *mockSyncable) SetSyncSource(s Source)  { m.source = s }
func (m *mockSyncable) SyncDependencies() []string { return nil }

func TestDiffEngine_CreateNew(t *testing.T) {
	yaml := []*mockSyncable{
		{key: "a", hash: 100, source: SourceYAML},
	}
	db := []*mockSyncable{}

	engine := NewDiffEngine[*mockSyncable]("test", PolicyCreateOnly)
	entries := engine.Diff(yaml, db)

	if len(entries) != 1 {
		t.Fatalf("Expected 1 entry, got %d", len(entries))
	}

	if entries[0].Action != ActionCreate {
		t.Errorf("Expected CREATE action, got %v", entries[0].Action)
	}
}

func TestDiffEngine_SkipUnchanged(t *testing.T) {
	yaml := []*mockSyncable{
		{key: "a", hash: 100, source: SourceYAML},
	}
	db := []*mockSyncable{
		{key: "a", hash: 100, source: SourceYAML}, // Same hash
	}

	engine := NewDiffEngine[*mockSyncable]("test", PolicyFullSync)
	entries := engine.Diff(yaml, db)

	if len(entries) != 1 {
		t.Fatalf("Expected 1 entry, got %d", len(entries))
	}

	if entries[0].Action != ActionSkip {
		t.Errorf("Expected SKIP action for unchanged entity, got %v", entries[0].Action)
	}
}

func TestDiffEngine_UpdateChanged(t *testing.T) {
	yaml := []*mockSyncable{
		{key: "a", hash: 200, source: SourceYAML}, // Different hash
	}
	db := []*mockSyncable{
		{key: "a", hash: 100, source: SourceYAML},
	}

	engine := NewDiffEngine[*mockSyncable]("test", PolicyFullSync)
	entries := engine.Diff(yaml, db)

	if len(entries) != 1 {
		t.Fatalf("Expected 1 entry, got %d", len(entries))
	}

	if entries[0].Action != ActionUpdate {
		t.Errorf("Expected UPDATE action for changed entity, got %v", entries[0].Action)
	}
}

func TestDiffEngine_CreateOnlySkipsUpdates(t *testing.T) {
	yaml := []*mockSyncable{
		{key: "a", hash: 200, source: SourceYAML},
	}
	db := []*mockSyncable{
		{key: "a", hash: 100, source: SourceYAML},
	}

	engine := NewDiffEngine[*mockSyncable]("test", PolicyCreateOnly)
	entries := engine.Diff(yaml, db)

	if len(entries) != 1 {
		t.Fatalf("Expected 1 entry, got %d", len(entries))
	}

	if entries[0].Action != ActionSkip {
		t.Errorf("PolicyCreateOnly should skip updates, got %v", entries[0].Action)
	}
}

func TestDiffEngine_DeleteOrphans(t *testing.T) {
	yaml := []*mockSyncable{}
	db := []*mockSyncable{
		{key: "orphan", hash: 100, source: SourceYAML},
	}

	engine := NewDiffEngine[*mockSyncable]("test", PolicyFullSync)
	entries := engine.Diff(yaml, db)

	if len(entries) != 1 {
		t.Fatalf("Expected 1 entry, got %d", len(entries))
	}

	if entries[0].Action != ActionDelete {
		t.Errorf("Expected DELETE action for orphan, got %v", entries[0].Action)
	}
}

func TestDiffEngine_SourceAware(t *testing.T) {
	yaml := []*mockSyncable{
		{key: "yaml-entity", hash: 200, source: SourceYAML},
		{key: "api-entity", hash: 200, source: SourceYAML},
	}
	db := []*mockSyncable{
		{key: "yaml-entity", hash: 100, source: SourceYAML}, // Created from YAML
		{key: "api-entity", hash: 100, source: SourceAPI},   // Created via API
	}

	engine := NewDiffEngine[*mockSyncable]("test", PolicySourceAware)
	entries := engine.Diff(yaml, db)

	if len(entries) != 2 {
		t.Fatalf("Expected 2 entries, got %d", len(entries))
	}

	// Find entries by key
	var yamlEntry, apiEntry DiffEntry
	for _, e := range entries {
		if e.Key == "yaml-entity" {
			yamlEntry = e
		} else {
			apiEntry = e
		}
	}

	// YAML entity should be updated
	if yamlEntry.Action != ActionUpdate {
		t.Errorf("YAML entity should be updated, got %v", yamlEntry.Action)
	}

	// API entity should be skipped
	if apiEntry.Action != ActionSkip {
		t.Errorf("API entity should be skipped, got %v", apiEntry.Action)
	}
}

// =============================================================================
// Diff Stats Tests
// =============================================================================

func TestCalculateStats(t *testing.T) {
	entries := []DiffEntry{
		{Action: ActionCreate},
		{Action: ActionCreate},
		{Action: ActionUpdate},
		{Action: ActionDelete},
		{Action: ActionSkip},
		{Action: ActionSkip},
		{Action: ActionSkip},
	}

	stats := CalculateStats(entries)

	if stats.Creates != 2 {
		t.Errorf("Creates = %d, want 2", stats.Creates)
	}
	if stats.Updates != 1 {
		t.Errorf("Updates = %d, want 1", stats.Updates)
	}
	if stats.Deletes != 1 {
		t.Errorf("Deletes = %d, want 1", stats.Deletes)
	}
	if stats.Skipped != 3 {
		t.Errorf("Skipped = %d, want 3", stats.Skipped)
	}
	if stats.Total != 7 {
		t.Errorf("Total = %d, want 7", stats.Total)
	}
}

func TestDiffStats_HasChanges(t *testing.T) {
	tests := []struct {
		name    string
		stats   DiffStats
		want    bool
	}{
		{"only skipped", DiffStats{Skipped: 5}, false},
		{"with creates", DiffStats{Creates: 1}, true},
		{"with updates", DiffStats{Updates: 1}, true},
		{"with deletes", DiffStats{Deletes: 1}, true},
		{"mixed", DiffStats{Creates: 1, Skipped: 5}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.stats.HasChanges(); got != tt.want {
				t.Errorf("HasChanges() = %v, want %v", got, tt.want)
			}
		})
	}
}

// =============================================================================
// Engine Tests
// =============================================================================

func TestEngine_ReconcilerOrder(t *testing.T) {
	engine := NewEngine(nil)

	var order []string

	engine.Register("third", 30, func(ctx context.Context, policy Policy) (*SectionResult, error) {
		order = append(order, "third")
		return &SectionResult{Section: "third"}, nil
	})

	engine.Register("first", 10, func(ctx context.Context, policy Policy) (*SectionResult, error) {
		order = append(order, "first")
		return &SectionResult{Section: "first"}, nil
	})

	engine.Register("second", 20, func(ctx context.Context, policy Policy) (*SectionResult, error) {
		order = append(order, "second")
		return &SectionResult{Section: "second"}, nil
	})

	_, err := engine.Sync(context.Background())
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	expected := []string{"first", "second", "third"}
	for i, name := range expected {
		if order[i] != name {
			t.Errorf("Order[%d] = %s, want %s", i, order[i], name)
		}
	}
}

func TestEngine_PolicyInheritance(t *testing.T) {
	cfg := &EngineConfig{
		DefaultPolicy: PolicyCreateOnly,
		Policies: map[string]Policy{
			"tree":       PolicyFullSync,
			"tree.smart": PolicyIgnore,
		},
	}

	engine := NewEngine(cfg)

	tests := []struct {
		path string
		want Policy
	}{
		{"tree", PolicyFullSync},
		{"tree.views", PolicyFullSync},      // Inherits from tree
		{"tree.smart", PolicyIgnore},        // Specific override
		{"entities", PolicyCreateOnly},      // Uses default
		{"entities.targets", PolicyCreateOnly}, // Uses default
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got := engine.GetPolicy(tt.path)
			if got != tt.want {
				t.Errorf("GetPolicy(%s) = %v, want %v", tt.path, got, tt.want)
			}
		})
	}
}

func TestEngine_SkipsIgnoredReconcilers(t *testing.T) {
	engine := NewEngine(&EngineConfig{
		DefaultPolicy: PolicyIgnore,
	})

	called := false
	engine.Register("test", 10, func(ctx context.Context, policy Policy) (*SectionResult, error) {
		// This should still be called, but with PolicyIgnore
		called = true
		if policy != PolicyIgnore {
			t.Errorf("Expected PolicyIgnore, got %v", policy)
		}
		return &SectionResult{Section: "test", Skipped: 10}, nil
	})

	result, err := engine.Sync(context.Background())
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	if !called {
		t.Error("Reconciler should still be called (to report skip)")
	}

	if result.TotalSkipped != 10 {
		t.Errorf("TotalSkipped = %d, want 10", result.TotalSkipped)
	}
}
