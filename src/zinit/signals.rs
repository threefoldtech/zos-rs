use std::fmt::{Display, Formatter, Result};

pub enum Signals {
    SIGABRT,
    SIGALRM,
    SIGBUS,
    SIGCHLD,
    SIGCLD,
    SIGCONT,
    SIGFPE,
    SIGHUP,
    SIGILL,
}

impl Display for Signals {
    fn fmt(&self, f: &mut Formatter) -> Result {
        match self {
            Signals::SIGABRT => write!(f, "SIGABRT"),
            Signals::SIGALRM => write!(f, "SIGALRM"),
            Signals::SIGBUS => write!(f, "SIGBUS"),
            Signals::SIGCHLD => write!(f, "SIGCHLD"),
            Signals::SIGCLD => write!(f, "SIGCLD"),
            Signals::SIGCONT => write!(f, "SIGCONT"),
            Signals::SIGFPE => write!(f, "SIGFPE"),
            Signals::SIGHUP => write!(f, "SIGHUP"),
            Signals::SIGILL => write!(f, "SIGILL"),
        }
    }
}
