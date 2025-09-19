#!/bin/bash
set -e

echo "Building upload-mbtiles (Rust version)..."
cargo build --release

echo "Build complete! Binary available at: ./target/release/upload-mbtiles"
echo ""
echo "Usage:"
echo "  ./target/release/upload-mbtiles tiles.mbtiles s3://bucket/path/"
echo ""
echo "For help:"
echo "  ./target/release/upload-mbtiles --help"

