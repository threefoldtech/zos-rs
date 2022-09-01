use anyhow::{Context, Error, Result};
use std::env;
use std::{fmt::Display, str::FromStr};

use super::kernel;
lazy_static::lazy_static! {
    // #[allow(non_upper_case_globals)]
    // I wanted to call it `runtime` instead of RUNTIME
    // but seems the allow non_upper_case_globals does not work
    // with lazy_static for some reason.
    // TODO
    pub static ref RUNTIME: Environment = get().unwrap();
}

// possible Running modes
#[derive(Debug, Clone, PartialEq)]
pub enum RunMode {
    Dev,
    Qa,
    Test,
    Main,
}

impl Display for RunMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunMode::Dev => write!(f, "development"),
            RunMode::Qa => write!(f, "qa"),
            RunMode::Test => write!(f, "testing"),
            RunMode::Main => write!(f, "production"),
        }
    }
}

impl FromStr for RunMode {
    type Err = &'static str;

    fn from_str(input: &str) -> std::result::Result<RunMode, Self::Err> {
        match input {
            "dev" => Ok(RunMode::Dev),
            "development" => Ok(RunMode::Dev),
            "qa" => Ok(RunMode::Qa),
            "test" => Ok(RunMode::Test),
            "main" => Ok(RunMode::Main),
            "production" => Ok(RunMode::Main),
            _ => Err("invalid run mode"),
        }
    }
}
// Environment holds information about running environment of a node
// it defines the different constant based on the running mode (dev, test, prod)
#[derive(Debug, Clone)]
pub struct Environment {
    pub mode: RunMode,
    pub storage_url: String,
    pub bin_repo: String,
    pub farmer_id: Option<u32>,
    pub farmer_secret: Option<String>,
    pub substrate_url: Vec<String>,
    pub activation_url: String,
    pub extended_config_url: Option<String>,
}

fn default(run_mode: RunMode) -> Environment {
    Environment {
        storage_url: "redis://hub.grid.tf:9900".into(),
        farmer_id: None,
        extended_config_url: None,
        farmer_secret: None,
        mode: run_mode.clone(),
        bin_repo: match run_mode {
            RunMode::Dev => "tf-zos-v3-bins.dev".into(),
            RunMode::Qa => "tf-zos-v3-bins.qanet".into(),
            RunMode::Test => "tf-zos-v3-bins.test".into(),
            RunMode::Main => "tf-zos-v3-bins".into(),
        },
        substrate_url: match run_mode {
            RunMode::Dev => vec!["wss://tfchain.dev.grid.tf/".into()],
            RunMode::Qa => vec!["wss://tfchain.qa.grid.tf/".into()],
            RunMode::Test => vec!["wss://tfchain.test.grid.tf/".into()],
            RunMode::Main => vec![
                "wss://tfchain.grid.tf/".into(),
                "wss://02.tfchain.grid.tf/".into(),
                "wss://03.tfchain.grid.tf/".into(),
                "wss://04.tfchain.grid.tf/".into(),
            ],
        },
        activation_url: match run_mode {
            RunMode::Dev => "https://activation.dev.grid.tf/activation/activate".into(),
            RunMode::Qa => "https://activation.qa.grid.tf/activation/activate".into(),
            RunMode::Test => "https://activation.test.grid.tf/activation/activate".into(),
            RunMode::Main => "https://activation.grid.tf/activation/activate".into(),
        },
    }
}

fn get() -> Result<Environment> {
    let params = kernel::get();
    from_params(params)
}

fn from_params(params: kernel::Params) -> Result<Environment> {
    let mut run_mode: RunMode = match params.value("runmode") {
        Some(runmode) => runmode
            .parse()
            .map_err(Error::msg)
            .context("failed to parse runmode from kernel cmdline")?,
        None => RunMode::Main,
    };

    if let Ok(mode) = env::var("ZOS_RUNMODE") {
        run_mode = mode
            .parse()
            .map_err(Error::msg)
            .context("failed to parse runmode from ENV")?;
    };

    let mut env = default(run_mode);
    if let Some(extended) = params.value("config_url") {
        env.extended_config_url = Some(extended.into());
    }

    if let Some(substrate) = params.value("substrate") {
        env.substrate_url = vec![substrate.into()];
    };

    if let Some(activation) = params.value("activation") {
        env.activation_url = activation.into();
    }

    if let Some(secret) = params.value("secret") {
        env.farmer_secret = Some(secret.into());
    }

    if let Some(id) = params.value("farmer_id") {
        env.farmer_id = Some(id.parse().context("invalid farmer id not numeric")?);
    }

    // Checking if there environment variable
    // override default settings
    if let Ok(substrate_url) = env::var("ZOS_SUBSTRATE_URL") {
        // let urls: Vec<&str> =  substrate.iter().map(|s| s as &str).collect();
        env.substrate_url = vec![substrate_url];
    }

    if let Ok(flist_url) = env::var("ZOS_FLIST_URL") {
        env.storage_url = flist_url;
    }

    if let Ok(bin_repo) = env::var("ZOS_BIN_REPO") {
        env.bin_repo = bin_repo;
    };

    Ok(env)
}

#[cfg(test)]
mod test {
    use crate::env::RunMode;

    #[test]
    fn get_env() {
        use super::RUNTIME;
        assert_eq!(RUNTIME.mode, RunMode::Main);
        assert_eq!(
            RUNTIME.activation_url,
            "https://activation.grid.tf/activation/activate"
        );
        assert_eq!(RUNTIME.substrate_url.len(), 4);
    }
}
