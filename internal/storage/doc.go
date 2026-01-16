// Package storage implements a high-performance time-series storage system
// for the Stalker network monitoring platform.
//
// Architecture:
//
//	┌─────────────┐     ┌─────────────┐     ┌─────────────┐
//	│  Ingestion  │────▶│   Buffer    │────▶│   Parquet   │
//	│   Service   │     │ (Ring/WAL)  │     │   Writer    │
//	└─────────────┘     └─────────────┘     └─────────────┘
//	                           │
//	                           ▼
//	                    ┌─────────────┐
//	                    │  Aggregate  │
//	                    │   Manager   │
//	                    └─────────────┘
//
// The storage system provides:
//   - High-throughput ingestion (500k+ samples/sec)
//   - Tiered storage with automatic compaction (Raw → 5min → Hourly → Daily → Weekly)
//   - Parquet-based columnar storage for efficient queries
//   - DuckDB query engine for analytics
//   - DDSketch-based percentile calculation
//   - Configurable retention policies
//   - Backpressure handling
//
// See STORAGE_IMPLEMENTATION.md for implementation details.
package storage
