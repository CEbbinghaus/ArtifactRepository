use std::{collections::BTreeMap, net::SocketAddr, path::Path};

use common::constants::SERVER_PORT;
use figment::{
    providers::{Env, Format, Serialized},
    value::{Dict, Map},
    Figment, Metadata, Profile, Provider,
};
use serde::{Deserialize, Serialize};

use crate::Cli;

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    pub store: Option<StoreConfig>,
    #[serde(default)]
    pub logging: LoggingConfig,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ServerConfig {
    #[serde(default = "default_bind")]
    pub bind: SocketAddr,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: default_bind(),
        }
    }
}

fn default_bind() -> SocketAddr {
    SocketAddr::V4(std::net::SocketAddrV4::new(
        std::net::Ipv4Addr::new(0, 0, 0, 0),
        SERVER_PORT,
    ))
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "backend")]
pub enum StoreConfig {
    #[serde(rename = "fs")]
    Fs { root: String },
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct LoggingConfig {
    pub level: Option<LogLevel>,
    pub format: Option<OutputFormat>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Error,
    Warn,
    #[default]
    Info,
    Debug,
    Trace,
}

impl LogLevel {
    pub fn from_scalar(value: i8) -> Self {
        match value {
            i8::MIN..=-2 => LogLevel::Error,
            -1 => LogLevel::Warn,
            0 => LogLevel::Info,
            1 => LogLevel::Debug,
            2..=i8::MAX => LogLevel::Trace,
        }
    }
}

impl From<LogLevel> for tracing::Level {
    fn from(level: LogLevel) -> Self {
        match level {
            LogLevel::Error => tracing::Level::ERROR,
            LogLevel::Warn => tracing::Level::WARN,
            LogLevel::Info => tracing::Level::INFO,
            LogLevel::Debug => tracing::Level::DEBUG,
            LogLevel::Trace => tracing::Level::TRACE,
        }
    }
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            LogLevel::Error => "error",
            LogLevel::Warn => "warn",
            LogLevel::Info => "info",
            LogLevel::Debug => "debug",
            LogLevel::Trace => "trace",
        };
        write!(f, "{}", s)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum OutputFormat {
    #[default]
    Ansi,
    Plain,
    Json,
}

impl std::fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            OutputFormat::Ansi => "ansi",
            OutputFormat::Plain => "plain",
            OutputFormat::Json => "json",
        };
        write!(f, "{}", s)
    }
}

impl Config {
    pub fn load(path: Option<&Path>, cli: &Cli) -> anyhow::Result<Self> {
        let mut figment = Figment::new().merge(Serialized::defaults(Config::default()));

        if let Some(path) = path {
            figment = figment.merge(figment::providers::Toml::file(path));
        }

        figment = figment
            .merge(Env::prefixed("ARXSRV_").split("_"))
            .merge(cli);

        Ok(figment.extract()?)
    }
}

/// Take CLI args and map them into config values so they can override file/env settings.
impl Provider for Cli {
    fn metadata(&self) -> Metadata {
        Metadata::named("CLI arguments")
    }

    fn data(&self) -> figment::error::Result<Map<Profile, Dict>> {
        let mut dict = Dict::new();

        if let Some(store) = &self.store {
            let mut store_dict = BTreeMap::new();
            store_dict.insert(
                "backend".to_string(),
                figment::value::Value::from("fs".to_string()),
            );
            store_dict.insert(
                "root".to_string(),
                figment::value::Value::from(store.to_string_lossy().to_string()),
            );
            dict.insert("store".to_string(), figment::value::Value::from(store_dict));
        }

        {
            let mut logging_dict = BTreeMap::new();

            if self.quiet > 0 || self.verbose > 0 {
                let final_verbose = (self.verbose as i8).saturating_sub(self.quiet as i8);

                logging_dict.insert(
                    "level".to_string(),
                    figment::value::Value::from(LogLevel::from_scalar(final_verbose).to_string()),
                );
            }

            if self.plain.is_some() || self.json.is_some() {
                let format = if self.plain.unwrap_or(false) {
                    OutputFormat::Plain
                } else if self.json.unwrap_or(false) {
                    OutputFormat::Json
                } else {
                    OutputFormat::Ansi
                };

                logging_dict.insert(
                    "format".to_string(),
                    figment::value::Value::from(format.to_string()),
                );
            }

            dict.insert(
                "logging".to_string(),
                figment::value::Value::from(logging_dict),
            );
        }

        Ok(Profile::Default.collect(dict))
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn defaults_when_sections_missing() {
        let cfg: Config = toml::from_str("").unwrap();
        assert_eq!(
            cfg.server.bind,
            format!("0.0.0.0:{}", SERVER_PORT).parse().unwrap()
        );
        assert!(cfg.logging.level.is_none());
        assert!(cfg.logging.format.is_none());
        assert!(cfg.store.is_none());
    }

    #[test]
    fn cli_provider_emits_correct_values() {
        let cli = Cli {
            verbose: 2,
            quiet: 0,
            plain: Some(true),
            json: None,
            config: None,
            store: Some(PathBuf::from("/tmp/test-store")),
        };

        let config: Config = Figment::new()
            .merge(Serialized::defaults(Config::default()))
            .merge(&cli)
            .extract()
            .unwrap();

        assert!(
            matches!(&config.store, Some(StoreConfig::Fs { root }) if root == "/tmp/test-store")
        );
        assert!(matches!(&config.logging.format, Some(OutputFormat::Plain)));
        assert_eq!(config.logging.level, Some(LogLevel::Trace));
    }
}
