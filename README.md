# Upload MBTiles to S3 (Rust)

A high-performance Rust implementation for uploading MBTiles files to Amazon S3 with retry logic and progress tracking.

## Features

- **Async/Concurrent**: Uses Tokio for async operations and concurrent uploads
- **Retry Logic**: Exponential backoff with jitter for failed uploads
- **Progress Tracking**: Real-time progress reporting with ETA calculations
- **SQLite Optimizations**: Read-only optimizations for MBTiles file access
- **Header Support**: Configurable HTTP headers for different tile formats
- **Memory Efficient**: Streams tiles without loading everything into memory

## Installation

Make sure you have Rust installed, then build the project:

```bash
cargo build --release
```

## Usage

```bash
# Basic usage
./target/release/upload-mbtiles tiles.mbtiles s3://my-bucket/tiles/

# With options
./target/release/upload-mbtiles tiles.mbtiles s3://my-bucket/tiles/ \
  --min-zoom 0 \
  --max-zoom 14 \
  --threads 20 \
  --extension .pbf \
  --header "Cache-Control:max-age=3600" \
  --header "ACL:public-read"
```

## Command Line Options

- `mbtiles`: Path to the MBTiles file
- `s3_url`: S3 URL (e.g., s3://bucket/path)
- `--min-zoom`: Minimum zoom level (default: 0)
- `--max-zoom`: Maximum zoom level (default: 19)
- `--threads`: Number of concurrent uploads (default: 10)
- `--extension`: File extension for tiles (default: .pbf)
- `--header`: Additional HTTP headers (format: "key:value")

## Supported Headers

- `Content-Type`
- `Content-Encoding`
- `Cache-Control`
- `ACL`

## Default Headers by Extension

- `.pbf`: `Content-Encoding: gzip`, `Content-Type: application/vnd.mapbox-vector-tile`
- `.webp`: `Content-Type: image/webp`
- `.png`: `Content-Type: image/png`
- `.jpg/.jpeg`: `Content-Type: image/jpeg`

## AWS Configuration

The tool uses the standard AWS credential chain:
1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. AWS credentials file (`~/.aws/credentials`)
3. IAM roles (if running on EC2)

## Performance

This Rust implementation provides significant performance improvements over the Python version:
- **Memory efficiency**: Streaming processing without loading all tiles into memory
- **Concurrency**: True async concurrency with configurable thread limits
- **Error handling**: Robust retry logic with exponential backoff
- **SQLite optimizations**: Read-only database optimizations for faster tile access

## Error Handling

- Network errors are retried with exponential backoff
- SQLite errors are reported immediately
- Progress is maintained across retries
- Final error summary shows failed tile count