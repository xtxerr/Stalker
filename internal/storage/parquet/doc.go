// Package parquet implements Parquet file reading and writing for samples and aggregates.
//
// The package provides:
//   - SampleWriter/SampleReader for raw sample data
//   - AggregateWriter/AggregateReader for aggregated statistics
//   - Support for multiple compression algorithms (snappy, zstd, lz4, gzip)
//   - Type conversion between storage types and Parquet rows
package parquet
