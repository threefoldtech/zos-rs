use std::fmt::Display;

use thiserror::Error;

#[derive(Error, Debug)]
pub struct ExecError {
    pub code: u8,
    pub stderr: Vec<u8>,
}

impl Display for ExecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let msg = String::from_utf8(self.stderr.clone()).map_err(|_| std::fmt::Error)?;
        write!(f, "exited with error code {} - stderr: {}", self.code, msg)
    }
}

pub struct Command {
    cmd: String,
    args: Vec<String>,
}

impl Command {
    pub fn new<S: Into<String>>(cmd: S) -> Command {
        Command {
            cmd: cmd.into(),
            args: Vec::default(),
        }
    }

    pub fn arg<S: Into<String>>(&mut self, arg: S) -> &mut Self {
        self.args.push(arg.into());
        self
    }
}

#[async_trait::async_trait]
pub trait Executor {
    async fn run(&self, cmd: &Command) -> Result<Vec<u8>, ExecError>;
}

#[derive(Default)]
/// System is the default executor
/// that uses the tokio::process module
/// to implement the executor trait.
pub struct System;

#[async_trait::async_trait]
impl Executor for System {
    async fn run(&self, cmd: &Command) -> Result<Vec<u8>, ExecError> {
        unimplemented!()
    }
}

/// Mock implements the Executor trait
/// but will be used for testing.
#[cfg(test)]
pub struct Mock;

#[cfg(test)]
#[async_trait::async_trait]
impl Executor for Mock {
    async fn run(&self, cmd: &Command) -> Result<Vec<u8>, ExecError> {
        unimplemented!()
    }
}
