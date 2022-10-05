pub use nix::mount::{MntFlags, MsFlags};
use std::ffi::OsString;
use std::fmt::Display;
use std::path::Path;
use thiserror::Error;
use tokio::process::Command as TokioCommand;

#[derive(Error, Debug)]
pub enum Error {
    Spawn(#[from] std::io::Error),
    Exit { code: i32, stderr: Vec<u8> },
    Unix(#[from] nix::Error),
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
            Error::Unix(ref err) => {
                write!(f, "{}", err)
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

#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
pub trait Executor {
    /// run runs a short lived command, and return it's output.
    /// you should not use this for long lived commands or commands
    /// that are expect to return a lot of output since all output
    /// is captured.
    async fn run(&self, cmd: &Command) -> Result<Vec<u8>, Error>;
}

/// Syscalls trait to help with testing operations that requires calls
/// to syscalls (over nix).
/// Unfortunately, the automock does not work with lifetime generic arguments
/// so we have to find another way to mock it.
pub trait Syscalls {
    fn mount<S: AsRef<Path>, T: AsRef<Path>, F: AsRef<str>, D: AsRef<str>>(
        &self,
        source: Option<S>,
        target: T,
        fstype: Option<F>,
        flags: MsFlags,
        data: Option<D>,
    ) -> Result<(), Error>;

    fn umount<T: AsRef<Path>>(&self, target: T, flags: Option<MntFlags>) -> Result<(), Error>;
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

impl Syscalls for System {
    fn mount<S: AsRef<Path>, T: AsRef<Path>, F: AsRef<str>, D: AsRef<str>>(
        &self,
        source: Option<S>,
        target: T,
        fstype: Option<F>,
        flags: MsFlags,
        data: Option<D>,
    ) -> Result<(), Error> {
        nix::mount::mount(
            source.as_ref().map(|v| v.as_ref()),
            target.as_ref(),
            fstype.as_ref().map(|f| f.as_ref()),
            flags,
            data.as_ref().map(|d| d.as_ref()),
        )?;

        //nix::mount::umount2(target, flags)
        Ok(())
    }

    fn umount<T: AsRef<Path>>(&self, target: T, flags: Option<MntFlags>) -> Result<(), Error> {
        match flags {
            Some(flags) => nix::mount::umount2(target.as_ref(), flags)?,
            None => nix::mount::umount(target.as_ref())?,
        };
        Ok(())
    }
}

pub struct Mockyscalls;
impl Syscalls for Mockyscalls {
    fn mount<S: AsRef<Path>, T: AsRef<Path>, F: AsRef<str>, D: AsRef<str>>(
        &self,
        _source: Option<S>,
        _target: T,
        _fstype: Option<F>,
        _flags: MsFlags,
        _data: Option<D>,
    ) -> Result<(), Error> {
        Ok(())
    }

    fn umount<T: AsRef<Path>>(&self, _target: T, _flags: Option<MntFlags>) -> Result<(), Error> {
        Ok(())
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
