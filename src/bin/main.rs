mod modules;

use clap_v3::App;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let matches = App::new("Zero-OS")
    .version("1.0")
    .about("0-OS is an autonomous operating system design to expose raw compute, storage and network capacity.")
    .subcommand(
            App::new("zui")
                .about("Show Zero os UI")
                .version("1.0")

        )
        .get_matches();

    match matches.subcommand() {
        ("zui", Some(_sub_m)) => modules::zui::run().await?,
        _ => {
            println!("Welcome to zos, please supply subcommand or --help or more info")
        }
    }
    Ok(())
}
