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

    /// HTTP status carried by the error, when it originated from a Hub/CAS response.
    pub fn status(&self) -> Option<u16> {
        match self {
            Self::Hub { status, .. } => *status,
            _ => None,
        }
    }

    /// Errno to surface to FUSE clients. Maps known Hub/CAS HTTP statuses to a
    /// meaningful errno so an importing app (e.g. Radarr/Sonarr) can tell a quota
    /// or storage reject apart from a generic I/O failure. Everything else stays EIO.
    pub fn to_errno(&self) -> i32 {
        match self.status() {
            // Payload too large / insufficient storage: surface as "no space left"
            // so the client stops retrying into a quota wall instead of looping.
            Some(413 | 507) => libc::ENOSPC,
            Some(403) => libc::EACCES,
            // Rate-limited: transient, signal "try again".
            Some(429) => libc::EAGAIN,
            _ => libc::EIO,
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

impl From<xet_data::DataError> for Error {
    fn from(err: xet_data::DataError) -> Self {
        // Preserve the HTTP status from CAS client errors (e.g. 413/507 on a quota
        // reject during upload) so it can be surfaced in logs and mapped to a
        // meaningful errno. Without this the status is flattened into an opaque string.
        if let xet_data::DataError::ClientError(client_error) = &err
            && let Some(status) = client_error.status()
        {
            return Self::hub_status(status.as_u16(), err.to_string());
        }
        Self::Xet(err.to_string())
    }
}

impl From<xet_data::file_reconstruction::FileReconstructionError> for Error {
    fn from(err: xet_data::file_reconstruction::FileReconstructionError) -> Self {
        Self::Xet(err.to_string())
    }
}

pub fn is_retryable_status(status: u16) -> bool {
    matches!(status, 408 | 429 | 500 | 502 | 503 | 504)
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_errno_maps_known_statuses() {
        assert_eq!(Error::hub_status(413, "payload too large").to_errno(), libc::ENOSPC);
        assert_eq!(Error::hub_status(507, "insufficient storage").to_errno(), libc::ENOSPC);
        assert_eq!(Error::hub_status(403, "forbidden").to_errno(), libc::EACCES);
        assert_eq!(Error::hub_status(429, "rate limited").to_errno(), libc::EAGAIN);
        // Unknown status and statusless errors stay generic.
        assert_eq!(Error::hub_status(500, "server error").to_errno(), libc::EIO);
        assert_eq!(Error::hub("no status").to_errno(), libc::EIO);
        assert_eq!(Error::Xet("opaque".into()).to_errno(), libc::EIO);
    }

    #[test]
    fn status_only_set_for_hub_with_status() {
        assert_eq!(Error::hub_status(413, "x").status(), Some(413));
        assert_eq!(Error::hub("x").status(), None);
        assert_eq!(Error::Xet("x".into()).status(), None);
    }
}
