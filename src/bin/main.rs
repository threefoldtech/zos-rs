mod modules;

use clap::{Parser, Subcommand};
use std::error::Error;
use std::path::Path;
/// binary name of zos this one need to always match the one defined in cargo.toml
/// todo! find a way to read this in compile time.
const BIN_NAME: &str = "zos";
const GIT_VERSION: &str =
    git_version::git_version!(args = ["--tags", "--always", "--dirty=-modified"]);

#[derive(Parser)]
#[command(author, version = GIT_VERSION, about, long_about = None)]
struct Cli {
    /// Enable debug mode
    #[arg(short, long, global = true)]
    debug: bool,

    /// zbus broker
    #[arg(short, long, global = true, default_value_t = String::from("redis://127.0.0.1:6379"))]
    broker: String,

    /// Sub command
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// run zos ZUI
    ZUI,
    /// run storage daemon
    #[command(name = "storaged")]
    Storage,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // read the name of the executable
    let first = std::env::args_os()
        .map(|v| Path::new(&v).file_name().unwrap().to_owned())
        .next();

    // this is to support linking a subcommand to zos binary and execute
    // the subcommand directly
    // ln -s zos zui
    // this running ./zui will run zui subcommand directly
    let args = match first {
        Some(v) if v == BIN_NAME => Cli::parse(),
        Some(v) => {
            let i = [BIN_NAME.into(), v]
                .into_iter()
                .chain(std::env::args_os().skip(1));
            Cli::parse_from(i)
        }
        None => Cli::parse(),
    };

    let mut level = log::LevelFilter::Info;
    if args.debug {
        level = log::LevelFilter::Debug;
    }

    simple_logger::SimpleLogger::new()
        .with_utc_timestamps()
        .with_level(level)
        .init()
        .unwrap();

    let result = match args.command {
        Commands::ZUI => modules::zui::run(&args.broker).await,
        Commands::Storage => modules::storage::run(&args.broker).await,
    };

    if let Err(err) = result {
        log::error!("{:#}", err);
        std::process::exit(1);
    }

    Ok(())
}
