//! Client for the Hub's bucket live-follow event stream.
//!
//! `GET /api/buckets/{namespace}/{repo}/events` is a server-sent-events feed
//! of per-file changes (add/update/delete), letting the mount apply remote
//! changes as they happen instead of re-listing every loaded directory when
//! the bucket's `updatedAt` moves (the 30s poll fan-out). This module holds
//! the transport-level pieces: an incremental SSE parser, the typed events,
//! and the reqwest-backed stream. The reconnect/fallback policy lives in the
//! poll loop (`virtual_fs::poll`), the HTTP request in `HubApiClient`.

use std::collections::VecDeque;
use std::pin::Pin;

use futures::StreamExt;
use serde::Deserialize;
use tracing::warn;

use crate::error::{Error, Result};

// ── Typed events ───────────────────────────────────────────────────────

/// What happened to a path, per the feed's `changes` batches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FollowOp {
    Add,
    Update,
    Delete,
}

/// One per-file change from a `changes` batch. An `update` carries only the
/// fields that changed (an identical re-upload has just `uploadedAt`), so
/// every metadata field is optional: an absent field means "unchanged" (or,
/// for `xetHash`, possibly "not readable with this token") — never "cleared".
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FollowChange {
    pub path: String,
    pub op: FollowOp,
    #[serde(default)]
    pub size: Option<u64>,
    #[serde(default)]
    pub xet_hash: Option<String>,
    /// ISO8601 upload instant; present on every add and re-upload.
    #[serde(default)]
    pub uploaded_at: Option<String>,
    /// ISO8601 client-provided mtime. Explicitly `null` (deserialized to
    /// `None`, same as absent) when a re-upload cleared it — callers fall
    /// back to `uploaded_at`.
    #[serde(default)]
    pub mtime: Option<String>,
}

/// A parsed event from the live-follow feed.
#[derive(Debug, Clone)]
pub enum FollowEvent {
    /// Replay done, the stream now follows live. `cursor` is absent when the
    /// feed has seen no change yet.
    Ready { cursor: Option<String> },
    /// A batch of changes (coalesced server-side over ~200ms). `cursor` is
    /// the resume point strictly after this batch.
    Changes { cursor: String, changes: Vec<FollowChange> },
    /// The requested cursor/since is older than the server's buffer: the
    /// stream ends and the client must reconcile with a full re-list.
    Reset,
    /// Server-directed end of stream (rotation or shutdown): reconnect with
    /// the given cursor.
    Reconnect { cursor: Option<String> },
}

/// A live-follow stream handle. `Ok(None)` means the stream ended without a
/// server-directed `reconnect`/`reset` (TCP close, EOF) — per the server
/// contract the caller treats that exactly like `reconnect` with the last
/// cursor received; `Err` (transport error, read timeout) is handled the
/// same way.
#[async_trait::async_trait]
pub trait FollowStreamOps: Send {
    async fn next_event(&mut self) -> Result<Option<FollowEvent>>;
}

// ── SSE wire parsing ───────────────────────────────────────────────────

/// One raw server-sent event: the `event:` name and the joined `data:` lines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseEvent {
    pub event: String,
    pub data: String,
}

/// Incremental SSE parser: feed raw body chunks in whatever framing the
/// transport delivers, get back complete events. Handles lines split across
/// chunks, several events per chunk, CRLF endings, `:` comment lines (the
/// feed's 30s pings), and multi-line `data:` fields (joined with `\n` per the
/// SSE spec). Unknown fields (`id:`, `retry:`) are ignored.
#[derive(Default)]
pub struct SseParser {
    buf: Vec<u8>,
    /// Bytes before this offset hold no `\n`: a long line arriving in many
    /// chunks is scanned once, not once per chunk.
    scanned: usize,
    event: String,
    data: String,
}

impl SseParser {
    pub fn push(&mut self, chunk: &[u8]) -> Vec<SseEvent> {
        self.buf.extend_from_slice(chunk);
        let mut events = Vec::new();
        let mut consumed = 0;
        while let Some(pos) = self.buf[self.scanned..].iter().position(|&b| b == b'\n') {
            let end = self.scanned + pos;
            let line = String::from_utf8_lossy(&self.buf[consumed..end]);
            let line = line.trim_end_matches('\r');
            consumed = end + 1;
            self.scanned = consumed;

            if line.is_empty() {
                // Blank line dispatches the accumulated event.
                if !self.event.is_empty() || !self.data.is_empty() {
                    events.push(SseEvent {
                        event: std::mem::take(&mut self.event),
                        data: std::mem::take(&mut self.data),
                    });
                }
                continue;
            }
            if line.starts_with(':') {
                continue; // comment (keep-alive ping)
            }
            let (field, value) = match line.split_once(':') {
                // Exactly one leading space in the value is part of the framing.
                Some((field, value)) => (field, value.strip_prefix(' ').unwrap_or(value)),
                None => (line, ""),
            };
            match field {
                "event" => self.event = value.to_string(),
                "data" => {
                    if !self.data.is_empty() {
                        self.data.push('\n');
                    }
                    self.data.push_str(value);
                }
                _ => {}
            }
        }
        self.buf.drain(..consumed);
        self.scanned = self.buf.len();
        events
    }
}

/// Map a raw SSE event to a typed [`FollowEvent`]. Unknown event names and
/// malformed payloads yield `None` (logged) so a newer server can add event
/// types without breaking older clients.
pub fn parse_follow_event(raw: &SseEvent) -> Option<FollowEvent> {
    #[derive(Deserialize)]
    struct CursorOnly {
        #[serde(default)]
        cursor: Option<String>,
    }
    #[derive(Deserialize)]
    struct ChangesData {
        cursor: String,
        changes: Vec<FollowChange>,
    }
    // `ready`/`reconnect` may come with an empty payload.
    let cursor_data = if raw.data.is_empty() { "{}" } else { &raw.data };
    let parsed = match raw.event.as_str() {
        "ready" => serde_json::from_str::<CursorOnly>(cursor_data).map(|c| FollowEvent::Ready { cursor: c.cursor }),
        "changes" => serde_json::from_str::<ChangesData>(&raw.data).map(|d| FollowEvent::Changes {
            cursor: d.cursor,
            changes: d.changes,
        }),
        "reset" => Ok(FollowEvent::Reset),
        "reconnect" => {
            serde_json::from_str::<CursorOnly>(cursor_data).map(|c| FollowEvent::Reconnect { cursor: c.cursor })
        }
        other => {
            warn!("live-follow: ignoring unknown event type {other:?}");
            return None;
        }
    };
    match parsed {
        Ok(event) => Some(event),
        Err(e) => {
            // A `changes` payload can be large: log a prefix only.
            let head: String = raw.data.chars().take(200).collect();
            warn!(
                "live-follow: malformed {} payload ({e}), {} bytes: {head}",
                raw.event,
                raw.data.len()
            );
            None
        }
    }
}

// ── HTTP-backed stream ─────────────────────────────────────────────────

/// [`FollowStreamOps`] over a reqwest SSE response body. Change paths are
/// rebased below the mount's subfolder prefix (changes outside it are
/// dropped, but their batch still surfaces so the cursor keeps advancing).
pub struct HttpFollowStream {
    stream: Pin<Box<dyn futures::Stream<Item = std::result::Result<bytes::Bytes, reqwest::Error>> + Send>>,
    parser: SseParser,
    queued: VecDeque<FollowEvent>,
    path_prefix: String,
}

impl HttpFollowStream {
    pub fn new(response: reqwest::Response, path_prefix: String) -> Self {
        Self {
            stream: Box::pin(response.bytes_stream()),
            parser: SseParser::default(),
            queued: VecDeque::new(),
            path_prefix,
        }
    }

    /// Apply the subfolder prefix to a batch (see `HubApiClient::list_tree`,
    /// which strips the same prefix from tree entries).
    fn rebase(&self, changes: &mut Vec<FollowChange>) {
        if self.path_prefix.is_empty() {
            return;
        }
        changes.retain_mut(
            |change| match crate::hub_api::strict_descendant_rel(&change.path, &self.path_prefix) {
                Some(rel) => {
                    let start = change.path.len() - rel.len();
                    change.path.drain(..start);
                    true
                }
                None => false,
            },
        );
    }
}

#[async_trait::async_trait]
impl FollowStreamOps for HttpFollowStream {
    async fn next_event(&mut self) -> Result<Option<FollowEvent>> {
        loop {
            if let Some(event) = self.queued.pop_front() {
                return Ok(Some(event));
            }
            match self.stream.next().await {
                Some(Ok(chunk)) => {
                    for raw in self.parser.push(&chunk) {
                        if let Some(mut event) = parse_follow_event(&raw) {
                            if let FollowEvent::Changes { changes, .. } = &mut event {
                                self.rebase(changes);
                            }
                            self.queued.push_back(event);
                        }
                    }
                }
                Some(Err(e)) => return Err(Error::Http(e)),
                None => return Ok(None),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sse_parser_single_event() {
        let mut parser = SseParser::default();
        let events = parser.push(b"event: ready\ndata: {\"cursor\":\"c1\"}\n\n");
        assert_eq!(
            events,
            vec![SseEvent {
                event: "ready".into(),
                data: "{\"cursor\":\"c1\"}".into(),
            }]
        );
    }

    #[test]
    fn sse_parser_handles_chunked_lines() {
        // Field names, values, and the dispatching blank line can all be
        // split across transport chunks.
        let mut parser = SseParser::default();
        assert!(parser.push(b"eve").is_empty());
        assert!(parser.push(b"nt: chan").is_empty());
        assert!(parser.push(b"ges\ndata: {\"cursor\":\"c2\",\"chan").is_empty());
        assert!(parser.push(b"ges\":[]}\n").is_empty());
        let events = parser.push(b"\n");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event, "changes");
        assert_eq!(events[0].data, "{\"cursor\":\"c2\",\"changes\":[]}");
    }

    #[test]
    fn sse_parser_ignores_comments_and_crlf() {
        let mut parser = SseParser::default();
        // Ping comments (the feed's 30s keep-alives) produce nothing.
        assert!(parser.push(b": ping\n\n").is_empty());
        // CRLF line endings are accepted.
        let events = parser.push(b"event: reset\r\ndata: {\"reason\":\"cursor_too_old\"}\r\n\r\n");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event, "reset");
    }

    #[test]
    fn sse_parser_multiple_events_per_chunk() {
        let mut parser = SseParser::default();
        let events =
            parser.push(b"event: ready\ndata: {}\n\n: ping\n\nevent: reconnect\ndata: {\"cursor\":\"c9\"}\n\n");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event, "ready");
        assert_eq!(events[1].event, "reconnect");
    }

    #[test]
    fn sse_parser_joins_multi_line_data() {
        let mut parser = SseParser::default();
        let events = parser.push(b"event: x\ndata: a\ndata: b\n\n");
        assert_eq!(events[0].data, "a\nb");
    }

    #[test]
    fn parse_ready_without_cursor() {
        // `cursor` may be absent when the feed has seen no change yet; an
        // empty data payload is tolerated the same way.
        for data in ["{}", ""] {
            let event = parse_follow_event(&SseEvent {
                event: "ready".into(),
                data: data.into(),
            });
            assert!(
                matches!(event, Some(FollowEvent::Ready { cursor: None })),
                "data={data:?}"
            );
        }
    }

    #[test]
    fn parse_changes_batch() {
        let raw = SseEvent {
            event: "changes".into(),
            data: r#"{"cursor":"c3","changes":[
                {"path":"a/b.txt","op":"add","size":42,"xetHash":"h1","uploadedAt":"2026-05-01T00:00:00Z"},
                {"path":"a/c.txt","op":"update","uploadedAt":"2026-05-01T00:00:01Z","mtime":null,"mtimeNanos":null},
                {"path":"old.txt","op":"delete"}
            ]}"#
            .into(),
        };
        let Some(FollowEvent::Changes { cursor, changes }) = parse_follow_event(&raw) else {
            panic!("expected Changes");
        };
        assert_eq!(cursor, "c3");
        assert_eq!(changes.len(), 3);
        assert_eq!(changes[0].op, FollowOp::Add);
        assert_eq!(changes[0].size, Some(42));
        assert_eq!(changes[0].xet_hash.as_deref(), Some("h1"));
        // Update with only uploadedAt: everything else is None (null == absent).
        assert_eq!(changes[1].op, FollowOp::Update);
        assert_eq!(changes[1].size, None);
        assert_eq!(changes[1].mtime, None);
        assert_eq!(changes[2].op, FollowOp::Delete);
    }

    #[test]
    fn parse_reset_and_reconnect() {
        let reset = parse_follow_event(&SseEvent {
            event: "reset".into(),
            data: r#"{"reason":"cursor_too_old"}"#.into(),
        });
        assert!(matches!(reset, Some(FollowEvent::Reset)));
        let reconnect = parse_follow_event(&SseEvent {
            event: "reconnect".into(),
            data: r#"{"cursor":"c7"}"#.into(),
        });
        assert!(matches!(reconnect, Some(FollowEvent::Reconnect { cursor: Some(c) }) if c == "c7"));
    }

    #[test]
    fn parse_ignores_unknown_and_malformed() {
        assert!(
            parse_follow_event(&SseEvent {
                event: "new-fancy-event".into(),
                data: "{}".into(),
            })
            .is_none()
        );
        assert!(
            parse_follow_event(&SseEvent {
                event: "changes".into(),
                data: "not json".into(),
            })
            .is_none()
        );
    }
}
