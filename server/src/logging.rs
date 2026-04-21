use tracing_subscriber::{
	filter::LevelFilter, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer,
};

use crate::config::{LoggingConfig, OutputFormat};

pub fn configure_tracing(logging: &LoggingConfig) {
	let default_level = LevelFilter::from(Some(logging.level.unwrap_or_default().into()));

	let env_filter = EnvFilter::builder()
		.with_default_directive(default_level.into())
		.from_env_lossy();

	let tracing_layer = match logging.format.unwrap_or_default() {
		OutputFormat::Json => tracing_subscriber::fmt::layer().json().boxed(),
		OutputFormat::Plain => tracing_subscriber::fmt::layer().with_ansi(false).boxed(),
		OutputFormat::Ansi => tracing_subscriber::fmt::layer().with_ansi(true).boxed(),
	};

	tracing_subscriber::registry()
		.with(tracing_layer.with_filter(env_filter))
		.init();
}
