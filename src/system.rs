use std::ffi::OsString;
use std::fmt::Display;
use thiserror::Error;

#[derive(Error, Debug)]
pub struct Error {
    pub code: u8,
    pub stderr: Vec<u8>,
}

impl Error {
    // shortcut to create an error with str. this module
    // instead will create it like Error{code, stderr} directly
    // from command output.
    pub fn new<E: AsRef<str>>(code: u8, stderr: Option<E>) -> Error {
        Error {
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
        let msg = String::from_utf8(self.stderr.clone()).map_err(|_| std::fmt::Error)?;
        write!(f, "error-code: {} - message: {}", self.code, msg)
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct Command {
    cmd: OsString,
    args: Vec<OsString>,
}

impl Command {
    pub fn new<S: Into<OsString>>(cmd: S) -> Command {
        Command {
            cmd: cmd.into(),
            args: Vec::default(),
        }
    }

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

#[mockall::automock]
#[async_trait::async_trait]
pub trait Executor {
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
        unimplemented!("executing command: {}", cmd);
    }
}

// /// Mock implements the Executor trait
// /// but will be used for testing.
// #[cfg(test)]
// pub struct ExecutorMock;

// #[cfg(test)]
// #[async_trait::async_trait]
// impl Executor for ExecutorMock {
//     async fn run(&self, cmd: &Command) -> Result<Vec<u8>, ExecError> {
//         unimplemented!("mock execution of command: {}", cmd);
//     }
// }
