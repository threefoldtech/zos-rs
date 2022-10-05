use serde::{de::DeserializeOwned, Deserialize};
use serde_yaml::{self, Value};
use std::collections::HashMap;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
pub mod signals;
use signals::Signals;
use std::path::Path;

pub struct Client {
    socket: String,
}

const DEFAULT_SOCKET_PATH: &'static str = "/var/run/zinit.sock";
const DEFAULT_ZINIT_PATH: &'static str = "/etc/zinit";

#[derive(Deserialize)]
struct CommandResult {
    state: State,
    body: Value,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum State {
    Ok,
    Error,
}

#[derive(Deserialize)]
pub struct ServiceStatus {
    pub name: String,
    pub pid: u64,
    pub state: ServiceState,
    pub target: ServiceTarget,
    pub after: Vec<String>,
}

#[derive(Deserialize)]
pub struct ServiceState {
    pub state: PossibleState,
    pub reason: String,
}

#[derive(Deserialize)]
pub enum PossibleState {
    // ServiceStateUnknown is return when we cannot determine the status of a service
    ServiceStateUnknown,
    // ServiceStateRunning is return when we a service process is running and healthy
    ServiceStateRunning,
    // ServiceStateBlocked  is returned if the service can't start because of an unsatisfied dependency
    ServiceStateBlocked,
    // ServiceStateSpawned service has started, but zinit is not sure about its status yet.
    // this is usually a short-lived status, unless a test command is provided. In that case
    // the spawned state will only go to success if the test pass
    ServiceStateSpawned,
    // ServiceStateSuccess is return when a one shot service exited without errors
    ServiceStateSuccess,
    // ServiceStateError is return when we a service exit with an error (exit code != 0)
    ServiceStateError,
    //ServiceStateFailure is set of zinit can not spawn a service in the first place
    //due to a missing executable for example. Unlike `error` which is returned if the
    //service itself exits with an error.
    ServiceStateFailure,
}

#[derive(Deserialize)]
pub enum ServiceTarget {
    Up,
    Down,
}

#[derive(Deserialize)]
pub struct InitService {
    pub exec: String,
    pub oneshot: bool,
    pub test: String,
    pub after: Vec<String>,
    pub env: HashMap<String, String>,
    pub log: LogType,
}

#[derive(Deserialize)]
pub enum LogType {
    #[serde(rename = "stdout")]
    StdoutLogType,
    #[serde(rename = "ring")]
    RingLogType,
    #[serde(rename = "none")]
    NoneLogType,
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("invalid command")]
    InvalidCommand,

    #[error("error from remote: {0}")]
    Remote(String),

    #[error("{0}")]
    IO(#[from] std::io::Error),

    #[error("{0}")]
    Protocol(#[from] serde_yaml::Error),

    #[error("{0}")]
    Other(#[from] anyhow::Error),
}

impl Client {
    pub fn new<S: Into<String>>(address: S) -> Client {
        Client {
            socket: address.into(),
        }
    }

    async fn dial(self) -> Result<UnixStream> {
        let stream = UnixStream::connect(self.socket).await?;
        Ok(stream)
    }

    async fn cmd<C: AsRef<[u8]>, T: DeserializeOwned>(self, command: C) -> Result<T> {
        let cmd = command.as_ref();
        let mut stream = self.dial().await?;
        stream.write_all(cmd).await?;

        let mut data = String::new();
        stream.read_to_string(&mut data).await?;

        let res: CommandResult = serde_yaml::from_str(&data)?;

        if matches!(res.state, State::Error) {
            return Err(Error::Remote(serde_yaml::from_value(res.body)?));
        }

        Ok(serde_yaml::from_value(res.body)?)
    }

    pub async fn start<S: AsRef<str>>(self, service: S) -> Result<()> {
        self.cmd(format!("start {}", service.as_ref())).await
    }

    pub async fn stop<S: AsRef<str>>(self, service: S) -> Result<()> {
        self.cmd(format!("stop {}", service.as_ref())).await
    }

    pub async fn forget<S: AsRef<str>>(self, service: S) -> Result<()> {
        self.cmd(format!("forget {}", service.as_ref())).await
    }

    pub async fn kill<S: AsRef<str>>(self, service: S, signal: Signals) -> Result<()> {
        self.cmd(format!("kill {} {}", service.as_ref(), signal))
            .await
    }

    pub async fn reboot<S: AsRef<str>>(self, service: S) -> Result<()> {
        self.cmd(format!("reboot {}", service.as_ref())).await
    }

    pub async fn status<S: AsRef<str>>(self, service: S) -> Result<ServiceStatus> {
        self.cmd(format!("status {}", service.as_ref())).await
    }

    pub async fn list<S: AsRef<str>>(self, service: S) -> Result<Vec<ServiceState>> {
        self.cmd(format!("list {}", service.as_ref())).await
    }

    pub async fn exists<S: AsRef<str>>(self, service: S) -> Result<bool> {
        match self.status(service.as_ref()).await {
            Ok(_) => Ok(true),
            Err(err) => Err(err),
        }
    }

    pub async fn get<S: AsRef<str>>(self, service: S) -> Result<InitService> {
        let mut file = tokio::fs::OpenOptions::new()
            .read(true)
            .open(Path::new(&DEFAULT_ZINIT_PATH).join(format!("{}.yaml", service.as_ref())))
            .await?;

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;

        let init_service: InitService = serde_yaml::from_slice(&buffer)?;

        Ok(init_service)
    }
}

impl Default for Client {
    fn default() -> Client {
        Client {
            socket: DEFAULT_SOCKET_PATH.to_string(),
        }
    }
}
