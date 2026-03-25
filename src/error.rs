use std::fmt;

#[derive(Debug)]
pub enum Error {
    Hub { message: String, status: Option<u16> },
    Xet(String),
    Io(std::io::Error),
    Json(serde_json::Error),
    Http(reqwest::Error),
}

impl Error {
    pub fn hub(msg: impl Into<String>) -> Self {
        Self::Hub {
            message: msg.into(),
            status: None,
        }
    }

    pub fn hub_status(status: u16, msg: impl Into<String>) -> Self {
        Self::Hub {
            message: msg.into(),
            status: Some(status),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Hub {
                message,
                status: Some(s),
            } => write!(f, "Hub API error ({s}): {message}"),
            Self::Hub { message, status: None } => write!(f, "Hub API error: {message}"),
            Self::Xet(msg) => write!(f, "Xet error: {msg}"),
            Self::Io(err) => write!(f, "IO error: {err}"),
            Self::Json(err) => write!(f, "JSON error: {err}"),
            Self::Http(err) => write!(f, "HTTP error: {err}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Self::Json(err)
    }
}

impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        Self::Http(err)
    }
}

pub fn is_retryable_status(status: u16) -> bool {
    matches!(status, 408 | 429 | 500 | 502 | 503 | 504)
}

pub type Result<T> = std::result::Result<T, Error>;
