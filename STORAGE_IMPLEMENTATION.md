# Stalker Storage — Implementierungsplan

**Ziel:** Schrittweise Implementierung des Time-Series Storage-Systems in unabhängigen Modulen.

**Strategie:** Jedes Modul ist in sich abgeschlossen und kann in einer Chat-Session implementiert werden. Dieses Dokument dient als "Gedächtnis" zwischen Sessions.

**Referenz-Dokumente:**
- Design: `stalker_storage_design_overview.md`
- Ursprünglicher Plan: `stalker-storage-implementation-plan.md`

---

## Fortschritts-Tracker

| # | Modul | Status | Dateien | Session-Notizen |
|---|-------|--------|---------|-----------------|
| 1 | Project Setup | ✅ DONE | `internal/storage/` | Session 1: Verzeichnisse, doc.go, config example |
| 2 | Core Types | ✅ DONE | `internal/storage/types/*.go` | Session 1: Sample, Aggregate, Tier mit Tests |
| 3 | Configuration | ✅ DONE | `internal/storage/config/*.go` | Session 1: Config, Validate, Requirements |
| 4 | Streaming Aggregate | ✅ DONE | `internal/storage/aggregate/*.go` | Session 1: Aggregate, Manager mit DDSketch |
| 5 | Ring Buffer | ✅ DONE | `internal/storage/buffer/*.go` | Session 1: RingBuffer mit Query, Evict |
| 6 | WAL Writer | ✅ DONE | `internal/storage/wal/*.go` | Session 2: Writer, Reader, Encoding mit CRC32 |
| 7 | Parquet Writer | ✅ DONE | `internal/storage/parquet/*.go` | Session 2: Writer, Reader mit Compression |
| 8 | Ingestion Service | ✅ DONE | `internal/storage/ingestion/*.go` | Session 2: Service mit WAL, Buffer, Aggregation |
| 9 | Compaction Engine | ✅ DONE | `internal/storage/compaction/*.go` | Session 2: Engine mit Job-Queue, Re-Aggregation |
| 10 | Query Service | ✅ DONE | `internal/storage/query/*.go` | Session 2: DuckDB Query, Buffer Integration |
| 11 | Backpressure | ✅ DONE | `internal/storage/backpressure/*.go` | Session 2: Controller mit Hysteresis |
| 12 | Retention Manager | ✅ DONE | `internal/storage/retention/*.go` | Session 2: Cleanup, DryRun, DiskUsage |
| 13 | Protobuf Integration | ✅ DONE | `internal/storage/proto/*.go` | Session 3: Convert, ProtoTypes, Tests |
| 14 | Main Service | ✅ DONE | `internal/storage/service.go` | Session 2: Unified Service API |
| 15 | Integration Tests | ✅ DONE | `internal/storage/*_test.go` | Session 3: E2E Tests, Service Tests |

**Legende:** ⬜ TODO | 🔨 IN PROGRESS | ✅ DONE | ❌ BLOCKED

---

## Modul-Definitionen

### Modul 1: Project Setup

**Zweck:** Verzeichnisstruktur, Dependencies

**Status:** 🔨 IN PROGRESS

**Erstellt:**
```
internal/storage/
├── types/
├── config/
├── aggregate/
├── buffer/
├── wal/
├── parquet/
├── ingestion/
├── compaction/
├── query/
├── backpressure/
└── retention/
```

**Dependencies (in go.mod):**
- ✅ `github.com/DataDog/sketches-go v1.4.6` — DDSketch für Percentile
- ✅ `github.com/parquet-go/parquet-go v0.24.0` — Parquet Writer
- ✅ `github.com/marcboeker/go-duckdb v1.8.3` — Query Engine
- ✅ `go.uber.org/zap v1.27.0` — Logging

**Testkriterien:**
- [ ] `go build ./...` erfolgreich
- [ ] Alle Packages importierbar

---

### Modul 2: Core Types

**Zweck:** Gemeinsame Datentypen für alle Module

**Abhängigkeiten:** Modul 1

**Interface:**
```go
// internal/storage/types/sample.go
type Sample struct {
    Namespace   string
    Target      string
    Poller      string
    TimestampMs int64
    Value       float64
    Valid       bool
    Error       string
    PollMs      int32
}

// internal/storage/types/aggregate.go
type AggregateResult struct {
    Namespace   string
    Target      string
    Poller      string
    BucketStart int64
    BucketEnd   int64
    Count       int64
    Sum         float64
    Min         float64
    Max         float64
    Avg         float64
    P50         *float64
    P90         *float64
    P95         *float64
    P99         *float64
}

// internal/storage/types/tier.go
type Tier int
const (
    TierRaw Tier = iota
    Tier5Min
    TierHourly
    TierDaily
    TierWeekly
)
```

**Testkriterien:**
- [ ] Alle Typen kompilieren
- [ ] Tier.Duration() und Tier.String() korrekt

---

### Modul 3: Configuration

**Zweck:** YAML-Config laden, validieren, Defaults setzen

**Abhängigkeiten:** Modul 2

**Interface:**
```go
// internal/storage/config/config.go
type Config struct {
    DataDir    string           `yaml:"data_dir"`
    Scale      ScaleConfig      `yaml:"scale"`
    Features   FeaturesConfig   `yaml:"features"`
    Retention  RetentionConfig  `yaml:"retention"`
}

func Load(path string) (*Config, error)
func (c *Config) Validate() error
func DefaultConfig() *Config
```

**Testkriterien:**
- [ ] Beispiel-Config lädt fehlerfrei
- [ ] Validierung erkennt ungültige Werte

---

### Modul 4: Streaming Aggregate

**Zweck:** Laufende Aggregation mit optionalem DDSketch

**Abhängigkeiten:** Modul 2

**Interface:**
```go
// internal/storage/aggregate/aggregate.go
type StreamingAggregate struct { ... }

func New(enablePercentile bool) *StreamingAggregate
func (a *StreamingAggregate) Add(value float64)
func (a *StreamingAggregate) Reset(bucketStart int64)
func (a *StreamingAggregate) Result() types.AggregateResult
func (a *StreamingAggregate) Merge(other *StreamingAggregate)

// internal/storage/aggregate/manager.go
type AggregateManager struct { ... }

func NewManager(bucketSize time.Duration, enablePercentile bool) *AggregateManager
func (m *AggregateManager) Process(sample types.Sample)
func (m *AggregateManager) FlushCompleted() []types.AggregateResult
```

**Testkriterien:**
- [ ] Einzelnes Aggregat berechnet korrekt (AVG, MIN, MAX)
- [ ] DDSketch Percentile innerhalb 1% Genauigkeit
- [ ] Manager handelt 1M+ Poller

---

### Modul 5: Ring Buffer

**Zweck:** Lock-free In-Memory Buffer für Raw-Samples

**Abhängigkeiten:** Modul 2

**Interface:**
```go
// internal/storage/buffer/ringbuffer.go
type RingBuffer struct { ... }

func New(capacity int) *RingBuffer
func (rb *RingBuffer) Push(sample types.Sample) bool
func (rb *RingBuffer) Pop() (types.Sample, bool)
func (rb *RingBuffer) Len() int
func (rb *RingBuffer) Cap() int
func (rb *RingBuffer) UsageRatio() float64
func (rb *RingBuffer) Query(filter SampleFilter) []types.Sample
```

**Testkriterien:**
- [ ] FIFO-Verhalten korrekt
- [ ] Concurrent Push/Pop thread-safe
- [ ] Performance: >1M ops/sec

---

### Modul 6: WAL Writer

**Zweck:** Crash-safe Append-Only Log für Samples

**Abhängigkeiten:** Modul 2

**Interface:**
```go
// internal/storage/wal/writer.go
type Writer struct { ... }

func NewWriter(dir string, opts Options) (*Writer, error)
func (w *Writer) Write(samples []types.Sample) error
func (w *Writer) Sync() error
func (w *Writer) Close() error
func (w *Writer) Rotate() error

// internal/storage/wal/reader.go
type Reader struct { ... }

func NewReader(segmentPath string) (*Reader, error)
func (r *Reader) ReadAll() ([]types.Sample, error)
```

**Testkriterien:**
- [ ] Samples werden korrekt geschrieben/gelesen
- [ ] Recovery nach simuliertem Crash
- [ ] Performance: >100k samples/sec write

---

### Modul 7: Parquet Writer

**Zweck:** Schreibt Aggregate als Parquet-Files

**Abhängigkeiten:** Modul 2

**Interface:**
```go
// internal/storage/parquet/writer.go
type Writer struct { ... }

func NewWriter(path string, opts Options) (*Writer, error)
func (w *Writer) WriteAggregates(aggs []types.AggregateResult) error
func (w *Writer) WriteSamples(samples []types.Sample) error
func (w *Writer) Close() error
```

**Testkriterien:**
- [ ] Geschriebene Files sind valides Parquet
- [ ] DuckDB kann Files lesen
- [ ] Kompression funktioniert

---

### Modul 8: Ingestion Service

**Zweck:** Orchestriert Buffer, Aggregation, WAL, Flush

**Abhängigkeiten:** Module 4, 5, 6, 7

**Interface:**
```go
// internal/storage/ingestion/service.go
type Service struct { ... }

func New(config *config.Config) (*Service, error)
func (s *Service) Start() error
func (s *Service) Stop() error
func (s *Service) Ingest(samples []types.Sample) error
func (s *Service) Stats() ServiceStats
```

**Testkriterien:**
- [ ] Samples fließen durch Pipeline
- [ ] Graceful Shutdown funktioniert

---

### Modul 9: Compaction Engine

**Zweck:** Aggregiert Raw → 5min → Hourly → Daily → Weekly

**Abhängigkeiten:** Module 2, 7

**Interface:**
```go
// internal/storage/compaction/engine.go
type Engine struct { ... }

func New(config *config.Config) (*Engine, error)
func (e *Engine) Start() error
func (e *Engine) Stop() error
func (e *Engine) RunJob(job Job) error
func (e *Engine) ScheduleJobs() []Job
```

**Testkriterien:**
- [ ] Raw → 5min Aggregation korrekt
- [ ] Jobs sind idempotent

---

### Modul 10: Query Service

**Zweck:** DuckDB-basierte Queries auf Parquet-Files

**Abhängigkeiten:** Module 2, 3, 5

**Interface:**
```go
// internal/storage/query/service.go
type Service struct { ... }

func New(config *config.Config, buffer *buffer.RingBuffer) (*Service, error)
func (s *Service) QueryPoller(ctx context.Context, req PollerQuery) ([]types.AggregateResult, error)
func (s *Service) Close() error
```

**Testkriterien:**
- [ ] Tier-Selection funktioniert korrekt
- [ ] Hot-Data (Buffer) wird einbezogen
- [ ] Query-Performance <1s

---

### Modul 11: Backpressure

**Zweck:** Laststeuerung bei Überlast

**Abhängigkeiten:** Module 2, 5

**Interface:**
```go
// internal/storage/backpressure/controller.go
type Controller struct { ... }

func New(config *config.Config, buffer *buffer.RingBuffer) *Controller
func (c *Controller) CurrentLevel() Level
func (c *Controller) ShouldDrop() bool
func (c *Controller) ThrottleFactor() float64
```

**Testkriterien:**
- [ ] Level-Wechsel bei korrekten Thresholds
- [ ] Hysterese verhindert Flapping

---

### Modul 12: Retention Manager

**Zweck:** Löscht alte Daten nach Retention-Policy

**Abhängigkeiten:** Module 2, 3

**Interface:**
```go
// internal/storage/retention/manager.go
type Manager struct { ... }

func New(config *config.Config) *Manager
func (m *Manager) RunCleanup() CleanupResult
func (m *Manager) DryRun() CleanupResult
```

**Testkriterien:**
- [ ] Nur Files älter als Retention werden gelöscht
- [ ] DryRun löscht nichts

---

### Modul 13: Protobuf Integration

**Zweck:** Anbindung an bestehendes Stalker-Protokoll

**Abhängigkeiten:** Modul 2, `internal/proto`

**Interface:**
```go
// internal/storage/proto/convert.go
func SampleFromProto(pb *stalker.Sample) types.Sample
func SampleToProto(s types.Sample) *stalker.Sample
```

**Testkriterien:**
- [ ] Konvertierung hin/zurück verlustfrei

---

### Modul 14: Main Service

**Zweck:** Startet alle Komponenten

**Abhängigkeiten:** Alle Module

**Interface:**
```go
// internal/storage/service.go
type StorageService struct { ... }

func New(configPath string) (*StorageService, error)
func (s *StorageService) Start() error
func (s *StorageService) Stop() error
func (s *StorageService) Ingest(samples []types.Sample) error
func (s *StorageService) Query(...) (...)
```

---

### Modul 15: Integration Tests

**Zweck:** End-to-End-Tests der gesamten Pipeline

**Tests:**
- TestIngestAndQuery
- TestCompactionPipeline
- TestBackpressure
- TestRetention
- TestCrashRecovery

---

## Session-Protokoll

### Session 1 — 2025-01-15

**Module bearbeitet:** 1 (Project Setup), 2 (Core Types), 3 (Configuration), 4 (Streaming Aggregate), 5 (Ring Buffer)

**Ergebnis:**
- ✅ Verzeichnisstruktur unter `internal/storage/` angelegt
- ✅ Dependencies bereits in go.mod vorhanden
- ✅ STORAGE_IMPLEMENTATION.md erstellt
- ✅ doc.go für alle Packages erstellt
- ✅ storage.example.yaml Konfigurationsdatei erstellt
- ✅ Core Types implementiert:
  - `types/sample.go` - Sample und SampleBatch
  - `types/aggregate.go` - AggregateResult und AggregateBatch
  - `types/tier.go` - Tier enum mit Duration, Retention, TruncateToBucket
  - `types/types_test.go` - Vollständige Tests
- ✅ Configuration implementiert:
  - `config/config.go` - Alle Config-Structs, Load(), DefaultConfig()
  - `config/validate.go` - Validierung, EnsureDirectories()
  - `config/requirements.go` - CalculateRequirements(), FormatRequirements()
  - `config/config_test.go` - Vollständige Tests
- ✅ Streaming Aggregate implementiert:
  - `aggregate/aggregate.go` - StreamingAggregate mit DDSketch
  - `aggregate/manager.go` - AggregateManager für Bucket-Transitions
  - `aggregate/aggregate_test.go` - Tests inkl. Benchmarks
- ✅ Ring Buffer implementiert:
  - `buffer/ringbuffer.go` - Thread-safe RingBuffer mit Query, Evict
  - `buffer/ringbuffer_test.go` - Tests inkl. Concurrent-Tests

**Nächste Schritte:**
- `go build ./...` und `go test ./...` ausführen zur Verifizierung
- Modul 6 (WAL Writer) implementieren
- Modul 7 (Parquet Writer) implementieren

---

## Wie man eine Session startet

Wenn du eine neue Chat-Session beginnst, sage:

> "Ich möchte an der Stalker Storage Implementierung weiterarbeiten. 
> Bitte lies die Datei `STORAGE_IMPLEMENTATION.md` und setze beim nächsten 
> unerledigten Modul fort."

---

## Technische Notizen

### Parquet-Go Library
- Import: `github.com/parquet-go/parquet-go`
- Verwendet struct tags für Schema-Definition
- Compression: Snappy (default), Zstd, LZ4

### DuckDB-Go Binding
- Import: `github.com/marcboeker/go-duckdb`
- Verwendet `database/sql` Interface
- Parquet-Queries: `SELECT * FROM read_parquet('path/*.parquet')`

### DDSketch
- Import: `github.com/DataDog/sketches-go/ddsketch`
- Relative accuracy: 1% default
- Memory: ~2KB pro Sketch

### Stalker Proto Sample
- Definiert in `proto/stalker.proto`
- Felder: namespace, target, poller, timestamp_ms, value_gauge, valid, error, poll_ms
- Go package: `github.com/xtxerr/stalker/internal/proto`
