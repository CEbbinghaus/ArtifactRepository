use clap::Parser;
use common::store::Store;
use server::create_router;

use std::path::PathBuf;
use tower_http::compression::CompressionLayer;
use tower_http::trace::TraceLayer;

// --- Configuration types ---

#[derive(serde::Deserialize)]
struct Config {
    #[serde(default)]
    server: ServerConfig,
    store: StoreConfig,
    #[serde(default)]
    logging: LoggingConfig,
}

#[derive(serde::Deserialize)]
struct ServerConfig {
    #[serde(default = "default_bind")]
    bind: String,
    #[serde(default = "default_port")]
    port: u16,
}

fn default_bind() -> String {
    "0.0.0.0".into()
}
fn default_port() -> u16 {
    1287
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: default_bind(),
            port: default_port(),
        }
    }
}

#[derive(Debug, serde::Deserialize)]
#[serde(tag = "backend")]
enum StoreConfig {
    #[serde(rename = "fs")]
    Fs { root: String },
    #[serde(rename = "s3")]
    S3 {
        bucket: String,
        #[serde(default)]
        region: Option<String>,
        #[serde(default)]
        endpoint: Option<String>,
        #[serde(default)]
        access_key_id: Option<String>,
        #[serde(default)]
        secret_access_key: Option<String>,
        #[serde(default = "default_s3_root")]
        root: String,
    },
    #[serde(rename = "memory")]
    Memory,
}

fn default_s3_root() -> String {
    "/".into()
}

#[derive(serde::Deserialize)]
struct LoggingConfig {
    #[serde(default = "default_log_level")]
    level: String,
}

fn default_log_level() -> String {
    "info".into()
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
        }
    }
}

// --- Store builder ---

fn build_store(config: &StoreConfig) -> anyhow::Result<Store> {
    match config {
        StoreConfig::Fs { root } => {
            let path = std::path::Path::new(root);
            if !path.exists() {
                std::fs::create_dir_all(path)?;
            }
            Store::from_fs_path(path)
        }
        StoreConfig::S3 {
            bucket,
            region,
            endpoint,
            access_key_id,
            secret_access_key,
            root,
        } => {
            let mut builder = opendal::services::S3::default().bucket(bucket).root(root);
            if let Some(region) = region {
                builder = builder.region(region);
            }
            if let Some(endpoint) = endpoint {
                builder = builder.endpoint(endpoint);
            }
            if let Some(key) = access_key_id {
                builder = builder.access_key_id(key);
            }
            if let Some(secret) = secret_access_key {
                builder = builder.secret_access_key(secret);
            }
            Store::from_builder(builder)
        }
        StoreConfig::Memory => Store::from_builder(opendal::services::Memory::default()),
    }
}

// --- CLI ---

#[derive(Parser)]
#[clap(version, about = "Artifact repository server")]
pub struct Cli {
    /// Path to TOML configuration file
    #[arg(short, long)]
    pub config: Option<PathBuf>,

    /// Port to listen on (overrides config)
    #[arg(short, long)]
    pub port: Option<u16>,

    /// Increase verbosity (can be repeated, overrides config)
    #[arg(short, long, action = clap::ArgAction::Count)]
    pub verbose: u8,

    /// Path to the object store directory (uses fs backend, overrides config)
    pub store: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let config = if let Some(config_path) = &cli.config {
        let content = std::fs::read_to_string(config_path)?;
        toml::from_str::<Config>(&content)?
    } else if let Some(store_path) = &cli.store {
        Config {
            server: ServerConfig::default(),
            store: StoreConfig::Fs {
                root: store_path.to_str().unwrap().to_string(),
            },
            logging: LoggingConfig::default(),
        }
    } else {
        anyhow::bail!("Either --config or a store path must be provided");
    };

    // Determine log level: RUST_LOG > --verbose > config
    let log_level = if std::env::var("RUST_LOG").is_ok() {
        None
    } else if cli.verbose > 0 {
        Some(match cli.verbose {
            1 => "info",
            2 => "debug",
            _ => "trace",
        })
    } else {
        Some(config.logging.level.as_str())
    };

    let filter = if let Some(level) = log_level {
        tracing_subscriber::EnvFilter::new(level)
    } else {
        tracing_subscriber::EnvFilter::from_default_env()
    };

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    let port = cli.port.unwrap_or(config.server.port);
    let bind = config.server.bind.clone();

    tracing::info!(port = port, bind = %bind, "starting server");
    tracing::debug!(store_backend = ?config.store, "store configured");

    let store = build_store(&config.store)?;

    let compression_layer = CompressionLayer::new()
        .br(true)
        .deflate(true)
        .gzip(true)
        .zstd(true);

    let app = create_router(store)
        .layer(compression_layer)
        .layer(TraceLayer::new_for_http());

    let listener = tokio::net::TcpListener::bind((bind.as_str(), port)).await?;
    tracing::info!("Listening at http://{}", listener.local_addr()?);
    axum::serve(listener, app).await?;

    Ok(())
}
