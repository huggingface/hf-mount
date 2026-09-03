use std::fmt;

#[derive(Debug)]
pub enum Error {
    Hub {
        message: String,
        status: Option<u16>,
        /// Server-requested wait (IETF `RateLimit` header) on a rate-limited
        /// response, so outer retry loops honor the Hub's reset time.
        retry_after: Option<std::time::Duration>,
    },
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
            retry_after: None,
        }
    }

    pub fn hub_status(status: u16, msg: impl Into<String>) -> Self {
        Self::Hub {
            message: msg.into(),
            status: Some(status),
            retry_after: None,
        }
    }

    /// How long the server asked us to wait before retrying, when it said.
    pub fn retry_after(&self) -> Option<std::time::Duration> {
        match self {
            Self::Hub { retry_after, .. } => *retry_after,
            _ => None,
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
            // Transient (rate limit, server timeout, 5xx overload, transport
            // connect/timeout): signal "try again" instead of a hard I/O
            // failure.
            _ if self.is_transient() => libc::EAGAIN,
            _ => libc::EIO,
        }
    }

    /// Whether retrying this error can plausibly succeed. Single source of
    /// truth for "transient", shared with `send_with_retry`: HTTP statuses
    /// defer to `is_retryable_status`; transport errors are transient only
    /// for connect/timeout failures (decode or TLS failures are permanent).
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Http(e) => is_transient_http(e),
            _ => self.status().is_some_and(is_retryable_status),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Hub {
                message,
                status: Some(s),
                ..
            } => write!(f, "Hub API error ({s}): {message}"),
            Self::Hub {
                message, status: None, ..
            } => write!(f, "Hub API error: {message}"),
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

/// Transport failures worth retrying: connect and timeout only (decode or
/// TLS failures are permanent).
pub fn is_transient_http(err: &reqwest::Error) -> bool {
    err.is_timeout() || err.is_connect()
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
        assert_eq!(Error::hub_status(408, "request timeout").to_errno(), libc::EAGAIN);
        assert_eq!(Error::hub_status(503, "overloaded").to_errno(), libc::EAGAIN);
        // Non-retryable status and statusless errors stay generic.
        assert_eq!(Error::hub_status(501, "not implemented").to_errno(), libc::EIO);
        assert_eq!(Error::hub("no status").to_errno(), libc::EIO);
        assert_eq!(Error::Xet("opaque".into()).to_errno(), libc::EIO);
    }

    #[test]
    fn retry_after_only_carried_by_hub_errors() {
        let delay = std::time::Duration::from_secs(30);
        let hinted = Error::Hub {
            message: "x".into(),
            status: Some(429),
            retry_after: Some(delay),
        };
        assert_eq!(hinted.retry_after(), Some(delay));
        assert_eq!(Error::hub_status(429, "x").retry_after(), None);
        assert_eq!(Error::Xet("x".into()).retry_after(), None);
    }

    #[test]
    fn status_only_set_for_hub_with_status() {
        assert_eq!(Error::hub_status(413, "x").status(), Some(413));
        assert_eq!(Error::hub("x").status(), None);
        assert_eq!(Error::Xet("x".into()).status(), None);
    }
}
