use clap::Parser;
use common::store::Store;
use server::create_router;

use std::{
    fs::create_dir,
    path::PathBuf,
};
use tower_http::compression::CompressionLayer;

#[derive(Parser)]
#[clap(version, about, long_about = None)]
pub struct Cli {
    /// Increase verbosity, and can be used multiple times
    #[arg(short, long, action = clap::ArgAction::Count)]
    pub verbose: u8,

    #[arg(short, long, default_value_t = 1287)]
    pub port: u16,

    pub store: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Cli { store, port, .. } = Cli::parse();

    if !store.exists() {
        create_dir(&store).expect("Failed to create store directory");
    }

    let store = opendal::services::Fs::default().root(store.to_str().expect("valid path"));
    let store = Store::from_builder(store).expect("Failed to create store");

    let compression_layer: CompressionLayer = CompressionLayer::new()
        .br(true)
        .deflate(true)
        .gzip(true)
        .zstd(true);

    let app = create_router(store).layer(compression_layer);

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;

    println!("Listening at http://{}", listener.local_addr()?);
    axum::serve(listener, app).await?;

    Ok(())
}
