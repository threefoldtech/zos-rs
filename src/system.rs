use std::ffi::OsString;
use std::fmt::Display;
use thiserror::Error;
use tokio::process::Command as TokioCommand;

#[derive(Error, Debug)]
pub enum Error {
    Spawn(#[from] std::io::Error),
    Exit { code: i32, stderr: Vec<u8> },
}

impl Error {
    // shortcut to create an error with str. this module
    // instead will create it like Error{code, stderr} directly
    // from command output.
    pub fn new<E: AsRef<str>>(code: i32, stderr: Option<E>) -> Error {
        Error::Exit {
            code: code,
            stderr: match stderr {
                None => Vec::default(),
                Some(msg) => msg.as_ref().into(),
            },
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Exit {
                ref code,
                ref stderr,
            } => {
                let msg = String::from_utf8(stderr.clone()).map_err(|_| std::fmt::Error)?;
                write!(f, "error-code: {} - message: {}", code, msg)
            }
            Error::Spawn(ref err) => {
                write!(f, "failed to spawn command: {}", err)
            }
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct Command {
    cmd: OsString,
    args: Vec<OsString>,
}

impl Command {
    /// create a new command with given name
    pub fn new<S: Into<OsString>>(cmd: S) -> Command {
        Command {
            cmd: cmd.into(),
            args: Vec::default(),
        }
    }

    /// append an argument to a command
    pub fn arg<S: Into<OsString>>(mut self, arg: S) -> Self {
        self.args.push(arg.into());
        self
    }
}

impl Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.cmd)?;
        for arg in self.args.iter() {
            write!(f, " {:?}", arg)?;
        }

        Ok(())
    }
}

impl From<&Command> for TokioCommand {
    fn from(cmd: &Command) -> Self {
        let mut exe = TokioCommand::new(&cmd.cmd);
        for arg in cmd.args.iter() {
            exe.arg(arg);
        }

        exe
    }
}

#[mockall::automock]
#[async_trait::async_trait]
pub trait Executor {
    /// run runs a short lived command, and return it's output.
    /// you should not use this for long lived commands or commands
    /// that are expect to return a lot of output since all output
    /// is captured.
    async fn run(&self, cmd: &Command) -> Result<Vec<u8>, Error>;
}

#[derive(Default, Clone)]
/// System is the default executor
/// that uses the tokio::process module
/// to implement the executor trait.
pub struct System;

#[async_trait::async_trait]
impl Executor for System {
    async fn run(&self, cmd: &Command) -> Result<Vec<u8>, Error> {
        let mut cmd: TokioCommand = cmd.into();
        let out = cmd.output().await?;
        if !out.status.success() {
            return Err(Error::Exit {
                code: out.status.code().unwrap_or(512),
                stderr: out.stderr,
            });
        }

        Ok(out.stdout)
    }
}

#[cfg(test)]
mod test {
    use super::{Command, Error, Executor, System};

    #[tokio::test]
    async fn system_run_success() {
        let cmd = Command::new("echo").arg("hello world");
        let out = System.run(&cmd).await.unwrap();
        assert!(String::from_utf8_lossy(&out) == "hello world\n");
    }

    #[tokio::test]
    async fn system_run_failure() {
        let cmd = Command::new("false");
        let out = System.run(&cmd).await;

        assert!(matches!(out, Err(Error::Exit{code, ..}) if code == 1));
    }

    #[tokio::test]
    async fn system_run_failure_stderr() {
        let cmd = Command::new("sh")
            .arg("-c")
            .arg("echo 'bye world' 1>&2 && exit 2");

        let out = System.run(&cmd).await;

        assert!(
            matches!(out, Err(Error::Exit{code, stderr}) if code == 2 && String::from_utf8_lossy(&stderr) == "bye world\n")
        );
    }
}
