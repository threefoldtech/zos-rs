fn main() {
    let version = git_version::git_version!(args = ["--tags", "--always", "--dirty=-modified"]);
    println!("cargo:rustc-env=GIT_VERSION={}", version);
}
