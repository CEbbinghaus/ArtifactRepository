use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

use crate::Cli;

enum OutputFormat {
    Ansi,
    Plain,
    Json,
}

impl From<&String> for OutputFormat {
    fn from(value: &String) -> Self {
        match value.to_lowercase().as_str() {
            "json" => OutputFormat::Json,
            "plain" => OutputFormat::Plain,
            _ => OutputFormat::Ansi,
        }
    }
}

pub fn configure_tracing(cli: &Cli) {
    let Cli {
        quiet,
        verbose,
        plain,
        json,
        ..
    } = cli;

    let output_format = if plain.unwrap_or(false) {
        OutputFormat::Plain
    } else if json.unwrap_or(false) {
        OutputFormat::Json
    } else {
        std::env::var("RUST_LOG_FORMAT")
            .as_ref()
            .map(OutputFormat::from)
            .unwrap_or(OutputFormat::Ansi)
    };

    // RUST_LOG wins if set; otherwise map -v and -q count to a level.
    // -q = error, "" = warn, -v = info, -vv = debug, -vvv = trace.
    let final_verbose = 1 + (*verbose as i8).saturating_sub(*quiet as i8);
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        // Why the hell do we need to pass this as a string?
        EnvFilter::new(match final_verbose {
            i8::MIN..=0 => tracing::Level::ERROR.as_str(),
            1 => tracing::Level::WARN.as_str(),
            2 => tracing::Level::INFO.as_str(),
            3 => tracing::Level::DEBUG.as_str(),
            4..=i8::MAX => tracing::Level::TRACE.as_str(),
        })
    });

    let tracing_layer = match output_format {
        OutputFormat::Json => tracing_subscriber::fmt::layer().json().boxed(),
        OutputFormat::Plain => tracing_subscriber::fmt::layer().with_ansi(false).boxed(),
        OutputFormat::Ansi => tracing_subscriber::fmt::layer().with_ansi(true).boxed(),
    };

    tracing_subscriber::registry()
        .with(tracing_layer.with_filter(env_filter))
        .init();
}
