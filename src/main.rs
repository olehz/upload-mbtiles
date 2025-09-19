use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use bytes::Bytes;
use clap::Parser;
use futures::stream::{self, StreamExt};
use log::{error, info, warn};
use rand::Rng;
use rusqlite::{Connection, OpenFlags};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use url::Url;

const UPLOAD_PROGRESS_INTERVAL: u64 = 300;

#[derive(Parser)]
#[command(name = "upload-mbtiles")]
#[command(about = "Upload MBTiles to S3 with retry logic and progress tracking")]
struct Args {
    /// Path to the MBTiles file
    #[arg(value_name = "MBTILES")]
    mbtiles: String,

    /// S3 URL (e.g., s3://bucket/path)
    #[arg(value_name = "S3_URL")]
    s3_url: String,

    /// Minimum zoom level
    #[arg(long, default_value = "0")]
    min_zoom: u32,

    /// Maximum zoom level
    #[arg(long, default_value = "19")]
    max_zoom: u32,

    /// Number of simultaneous uploads
    #[arg(long, default_value = "10")]
    threads: usize,

    /// File extension for tiles
    #[arg(long, default_value = ".pbf")]
    extension: String,

    /// Additional headers (format: "key:value")
    #[arg(long = "header", short = 'h')]
    headers: Vec<String>,
}

#[derive(Debug, Clone)]
struct Tile {
    zoom: u32,
    x: u32,
    y: u32,
    data: Bytes,
}

struct UploadStats {
    tile_count: AtomicU64,
    upload_count: AtomicU64,
    start_time: Instant,
}

impl UploadStats {
    fn new() -> Self {
        Self {
            tile_count: AtomicU64::new(0),
            upload_count: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    fn increment_upload(&self) -> u64 {
        self.upload_count.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn set_tile_count(&self, count: u64) {
        self.tile_count.store(count, Ordering::Relaxed);
    }


    fn get_tile_count(&self) -> u64 {
        self.tile_count.load(Ordering::Relaxed)
    }

    fn get_elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }
}

struct MBTilesReader {
    conn: Connection,
    min_zoom: u32,
    max_zoom: u32,
}

impl MBTilesReader {
    fn new(path: &str, min_zoom: u32, max_zoom: u32) -> Result<Self> {
        let conn = Connection::open_with_flags(
            format!("file:{}?mode=ro", path),
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
        )
        .context("Failed to open MBTiles file")?;

        // Apply read-only optimizations
        // Some pragmas return results, so we use prepare/execute to handle both cases
        conn.execute_batch(
            "PRAGMA synchronous=OFF;
             PRAGMA journal_mode=OFF;
             PRAGMA cache_size=50000;
             PRAGMA temp_store=MEMORY;
             PRAGMA mmap_size=268435456;"
        ).context("Failed to set SQLite pragmas")?;

        Ok(Self {
            conn,
            min_zoom,
            max_zoom,
        })
    }

    fn count_tiles(&self) -> Result<u64> {
        let mut stmt = self
            .conn
            .prepare("SELECT COUNT(1) FROM tiles WHERE zoom_level >= ? AND zoom_level <= ?")?;
        let count: i64 = stmt.query_row([self.min_zoom, self.max_zoom], |row| row.get(0))?;
        Ok(count as u64)
    }

    fn get_tiles(&self) -> Result<Vec<Tile>> {
        let mut stmt = self.conn.prepare(
            "SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles 
             WHERE zoom_level >= ? AND zoom_level <= ? 
             ORDER BY zoom_level, tile_column, tile_row",
        )?;

        let tile_iter = stmt.query_map([self.min_zoom, self.max_zoom], |row| {
            let zoom: u32 = row.get(0)?;
            let x: u32 = row.get(1)?;
            let y_tms: u32 = row.get(2)?;
            let data: Vec<u8> = row.get(3)?;

            // Convert TMS to XYZ tile scheme
            let y = ((1 << zoom) - y_tms) - 1;

            Ok(Tile {
                zoom,
                x,
                y,
                data: Bytes::from(data),
            })
        })?;

        let mut tiles = Vec::new();
        for tile in tile_iter {
            tiles.push(tile?);
        }

        Ok(tiles)
    }
}

async fn upload_tile_with_retry(
    s3_client: &S3Client,
    bucket: &str,
    key_template: &str,
    headers: &HashMap<String, String>,
    tile: Tile,
    stats: Arc<UploadStats>,
    max_retries: u32,
) -> Result<()> {
    let mut retries = 0;

    loop {
        match upload_single_tile(s3_client, bucket, key_template, headers, &tile).await {
            Ok(_) => {
                let upload_count = stats.increment_upload();
                if upload_count % UPLOAD_PROGRESS_INTERVAL == 0 {
                    let elapsed = stats.get_elapsed().as_secs();
                    let total_tiles = stats.get_tile_count();
                    let eta = if upload_count > 0 {
                        (elapsed * (total_tiles - upload_count)) / upload_count
                    } else {
                        0
                    };
                    
                    println!(
                        "{} {} / {}",
                        tile.zoom,
                        format_duration(elapsed),
                        format_duration(eta)
                    );
                }
                return Ok(());
            }
            Err(e) => {
                warn!(
                    "Upload failed for tile {}/{}/{}: {} (attempt {})",
                    tile.zoom, tile.x, tile.y, e, retries + 1
                );

                if retries >= max_retries {
                    error!(
                        "Failed to upload tile {}/{}/{} after {} retries",
                        tile.zoom, tile.x, tile.y, retries
                    );
                    return Err(e.context(format!(
                        "Failed to upload tile {}/{}/{} after {} retries",
                        tile.zoom, tile.x, tile.y, retries
                    )));
                }

                // Exponential backoff with jitter
                let delay_ms = (2_u64.pow(retries) * 1000) + rand::thread_rng().gen_range(0..100);
                sleep(Duration::from_millis(delay_ms)).await;
                retries += 1;
            }
        }
    }
}

async fn upload_single_tile(
    s3_client: &S3Client,
    bucket: &str,
    key_template: &str,
    headers: &HashMap<String, String>,
    tile: &Tile,
) -> Result<()> {
    let key = key_template
        .replace("{z}", &tile.zoom.to_string())
        .replace("{x}", &tile.x.to_string())
        .replace("{y}", &tile.y.to_string());

    let mut put_request = s3_client
        .put_object()
        .bucket(bucket)
        .key(&key)
        .body(ByteStream::from(tile.data.clone()));

    // Add headers
    if let Some(content_type) = headers.get("Content-Type") {
        put_request = put_request.content_type(content_type);
    }
    if let Some(content_encoding) = headers.get("Content-Encoding") {
        put_request = put_request.content_encoding(content_encoding);
    }
    if let Some(cache_control) = headers.get("Cache-Control") {
        put_request = put_request.cache_control(cache_control);
    }
    if let Some(acl) = headers.get("ACL") {
        put_request = put_request.acl(acl.parse().context("Invalid ACL value")?);
    }

    put_request
        .send()
        .await
        .context("Failed to upload tile to S3")?;

    Ok(())
}

fn parse_headers(header_strings: &[String]) -> Result<HashMap<String, String>> {
    let mut headers = HashMap::new();
    
    for header in header_strings {
        let parts: Vec<&str> = header.splitn(2, ':').collect();
        if parts.len() != 2 {
            return Err(anyhow::anyhow!("Invalid header format: {}", header));
        }
        
        let key = parts[0].trim();
        let value = parts[1].trim();
        
        if !matches!(key, "Cache-Control" | "Content-Type" | "Content-Encoding" | "ACL") {
            return Err(anyhow::anyhow!("Unsupported header: {}", key));
        }
        
        headers.insert(key.to_string(), value.to_string());
    }
    
    Ok(headers)
}

fn format_duration(seconds: u64) -> String {
    let hours = seconds / 3600;
    let minutes = (seconds % 3600) / 60;
    let secs = seconds % 60;
    format!("{}:{:02}:{:02}", hours, minutes, secs)
}

fn get_default_headers(extension: &str) -> HashMap<String, String> {
    let mut headers = HashMap::new();
    
    match extension {
        ".pbf" => {
            headers.insert("Content-Encoding".to_string(), "gzip".to_string());
            headers.insert("Content-Type".to_string(), "application/vnd.mapbox-vector-tile".to_string());
        }
        ".webp" => {
            headers.insert("Content-Type".to_string(), "image/webp".to_string());
        }
        ".png" => {
            headers.insert("Content-Type".to_string(), "image/png".to_string());
        }
        ".jpg" | ".jpeg" => {
            headers.insert("Content-Type".to_string(), "image/jpeg".to_string());
        }
        _ => {}
    }
    
    headers
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    
    let args = Args::parse();
    
    // Parse S3 URL
    let s3_url = Url::parse(&args.s3_url)
        .context("Invalid S3 URL")?;
    
    if s3_url.scheme() != "s3" {
        return Err(anyhow::anyhow!("URL must use s3:// scheme"));
    }
    
    let bucket = s3_url.host_str()
        .ok_or_else(|| anyhow::anyhow!("No bucket specified in S3 URL"))?;
    let key_prefix = s3_url.path().trim_start_matches('/');
    
    // Initialize AWS S3 client
    let config = aws_config::defaults(BehaviorVersion::latest())
        .load()
        .await;
    let s3_client = S3Client::new(&config);
    
    // Test S3 connection
    s3_client
        .list_buckets()
        .send()
        .await
        .context("Failed to connect to S3. Please check your AWS credentials.")?;
    
    // Parse headers
    let mut headers = parse_headers(&args.headers)?;
    let default_headers = get_default_headers(&args.extension);
    for (key, value) in default_headers {
        headers.entry(key).or_insert(value);
    }
    
    // Open MBTiles file
    let mbtiles_reader = MBTilesReader::new(&args.mbtiles, args.min_zoom, args.max_zoom)
        .context("Failed to open MBTiles file")?;
    
    let tile_count = mbtiles_reader.count_tiles()
        .context("Failed to count tiles")?;
    
    let tiles = mbtiles_reader.get_tiles()
        .context("Failed to read tiles")?;
    
    let stats = Arc::new(UploadStats::new());
    stats.set_tile_count(tile_count);
    
    let key_template = format!("{}{}/{}/{}{}", key_prefix, "{z}", "{x}", "{y}", args.extension);
    
    info!(
        "Uploading {} tiles from {} to s3://{}/{}",
        tile_count, args.mbtiles, bucket, key_template
    );
    
    // Upload tiles concurrently
    let upload_stream = stream::iter(tiles)
        .map(|tile| {
            let s3_client = s3_client.clone();
            let bucket = bucket.to_string();
            let key_template = key_template.clone();
            let headers = headers.clone();
            let stats = stats.clone();
            
            tokio::spawn(async move {
                upload_tile_with_retry(
                    &s3_client,
                    &bucket,
                    &key_template,
                    &headers,
                    tile,
                    stats,
                    3, // max_retries
                )
                .await
            })
        })
        .buffer_unordered(args.threads);
    
    // Wait for all uploads to complete
    let results: Vec<_> = upload_stream.collect().await;
    
    // Check for errors
    let mut error_count = 0;
    for result in results {
        match result {
            Ok(Ok(())) => {} // Success
            Ok(Err(e)) => {
                error!("Upload error: {}", e);
                error_count += 1;
            }
            Err(e) => {
                error!("Task error: {}", e);
                error_count += 1;
            }
        }
    }
    
    if error_count > 0 {
        return Err(anyhow::anyhow!("Failed to upload {} tiles", error_count));
    }
    
    let final_elapsed = stats.get_elapsed().as_secs();
    info!("Upload completed in {}s", final_elapsed);
    
    Ok(())
}
