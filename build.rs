use std::path::Path;

fn main() {
    let output = std::env::var("OUT_DIR").unwrap();
    let version = git_version::git_version!(args = ["--tags", "--always", "--dirty=-modified"]);
    std::fs::write(
        Path::new(&output).join("version.rs"),
        format!(r#"const GIT_VERSION: &str = "{}";"#, version),
    )
    .unwrap();
}
