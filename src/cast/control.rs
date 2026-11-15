//! `src/cast-control.js`: the control channel between a cast sender and a receiver
//! (`peartube-cast/v1`), on the shared request/reply channel (`control_channel`): one Protomux
//! channel on the peer connection the two nodes already share. A cast needs no connection, port
//! or server of its own, and its messages travel under that connection's Noise encryption.
//!
//! ```text
//! sender -> receiver   {id, type: 'pair' | 'subscribe' | 'cmd', ...}
//! receiver -> sender   {id, ok, result | error}, and {event: 'state', state} once subscribed
//! ```
//!
//! Any peer that connects to a receiver can open this channel, so the receiver decides per
//! request what it allows.

use std::time::Duration;

use protomux::Mux;

use super::CastError;
pub use crate::control_channel::{
    Control, ControlHandlers, DEFAULT_TIMEOUT, EventHandler, MAX_FRAME, OpenHandler, RequestFuture,
    RequestHandler, ServeHandle, decode_frame, encode_frame,
};
use crate::control_channel::{Spec, open_channel, serve_channel};

/// The Protomux protocol name.
pub const PROTOCOL: &str = "peartube-cast/v1";

static CAST: Spec = Spec {
    protocol: PROTOCOL,
    label: "Receiver",
    refused: "This peer does not take casts",
    duplicate: "A cast session is already open with this receiver",
    timed_out: "Timed out opening the cast channel",
};

impl From<crate::control_channel::ControlError> for CastError {
    fn from(error: crate::control_channel::ControlError) -> Self {
        CastError::new(error.message, 0)
    }
}

/// `serveControl(conn, { onRequest, onOpen, onClose })`: the receiver's half, answering every
/// sender that opens the channel on this connection. Returns a handle that stops answering.
pub fn serve_control(mux: &Mux, handlers: ControlHandlers) -> ServeHandle {
    serve_channel(mux, &CAST, handlers)
}

/// `openControl(conn, { onEvent, onClose }, timeoutMs)`: the sender's half. Resolves once the
/// receiver answers the open; a peer that is not a receiver refuses the channel, which is an error.
pub async fn open_control(
    mux: &Mux,
    handlers: ControlHandlers,
    timeout: Duration,
) -> Result<Control, CastError> {
    let handlers = ControlHandlers {
        on_request: None,
        ..handlers
    };
    Ok(open_channel(mux, &CAST, handlers, timeout).await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use protomux::StreamInfo;
    use protomux::memory::duplex;
    use serde_json::{Map, Value, json};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn pair_of_muxes() -> (Mux, Mux) {
        let ((ar, aw), (br, bw)) = duplex(64);
        let info = StreamInfo {
            is_initiator: true,
            ..StreamInfo::default()
        };
        let remote = StreamInfo::default();
        (Mux::new(ar, aw, info), Mux::new(br, bw, remote))
    }

    #[test]
    fn frames_are_length_prefixed_json_like_the_app() {
        // `c.json` is `utf8`: one compact uint length, then the JSON text.
        let value = json!({ "id": 1, "type": "pair", "code": "ABC123" });
        let text = br#"{"id":1,"type":"pair","code":"ABC123"}"#;
        let mut expected = vec![text.len() as u8];
        expected.extend_from_slice(text);
        assert_eq!(&encode_frame(&value).expect("encoded")[..], &expected[..]);
        assert_eq!(decode_frame(&expected), Some(value));
    }

    #[test]
    fn frames_drop_what_the_app_drops() {
        let frame = |text: &[u8]| {
            let mut bytes = vec![text.len() as u8];
            bytes.extend_from_slice(text);
            bytes
        };
        assert!(decode_frame(&frame(b"{\"type\":\"pair\"}")).is_some());
        assert!(decode_frame(&frame(b"null")).is_none());
        assert!(decode_frame(&frame(b"7")).is_none());
        assert!(decode_frame(&frame(b"[1]")).is_none());
        assert!(decode_frame(&frame(b"{broken")).is_none());
        assert!(
            decode_frame(b"\x02{").is_none(),
            "a short buffer is dropped"
        );
        assert!(decode_frame(&vec![b' '; MAX_FRAME + 1]).is_none());
    }

    #[tokio::test]
    async fn a_request_round_trips_with_json_fields() {
        let (sender_mux, receiver_mux) = pair_of_muxes();
        let seen = Arc::new(AtomicUsize::new(0));
        let count = seen.clone();
        let serve = serve_control(
            &receiver_mux,
            ControlHandlers {
                on_request: Some(Arc::new(move |message, _control| {
                    count.fetch_add(1, Ordering::AcqRel);
                    Box::pin(async move {
                        assert_eq!(message["type"], "pair");
                        assert_eq!(message["code"], "ABC123");
                        Ok(json!({ "token": "t" }))
                    })
                })),
                ..ControlHandlers::default()
            },
        );
        let control = open_control(
            &sender_mux,
            ControlHandlers::default(),
            Duration::from_secs(5),
        )
        .await
        .expect("channel opens");
        let mut payload = Map::new();
        payload.insert("code".to_owned(), json!("ABC123"));
        let reply = control
            .call("pair", payload, Duration::from_secs(5))
            .await
            .expect("pair answer");
        assert_eq!(reply, json!({ "token": "t" }));
        assert_eq!(seen.load(Ordering::Acquire), 1);
        control.close();
        drop(serve);
    }

    #[tokio::test]
    async fn a_failing_handler_answers_ok_false() {
        let (sender_mux, receiver_mux) = pair_of_muxes();
        let _serve = serve_control(
            &receiver_mux,
            ControlHandlers {
                on_request: Some(Arc::new(|_message, _control| {
                    Box::pin(async { Err("Invalid TV pairing code".to_owned()) })
                })),
                ..ControlHandlers::default()
            },
        );
        let control = open_control(
            &sender_mux,
            ControlHandlers::default(),
            Duration::from_secs(5),
        )
        .await
        .expect("channel opens");
        let error = control
            .call("pair", Map::new(), Duration::from_secs(5))
            .await
            .expect_err("refused");
        assert_eq!(error.message, "Invalid TV pairing code");
    }

    #[tokio::test]
    async fn a_peer_that_does_not_pair_refuses_the_open() {
        let (sender_mux, receiver_mux) = pair_of_muxes();
        // The node marks a connection ready once its protocols are attached; unknown opens are
        // then refused at once instead of held.
        receiver_mux.ready();
        let error = open_control(
            &sender_mux,
            ControlHandlers::default(),
            Duration::from_secs(5),
        )
        .await
        .expect_err("no receiver half");
        assert_eq!(error.message, "This peer does not take casts");
    }

    #[tokio::test]
    async fn closing_the_channel_fails_a_pending_call() {
        let (sender_mux, receiver_mux) = pair_of_muxes();
        let _serve = serve_control(
            &receiver_mux,
            ControlHandlers {
                // Never answers: the call must fail when the channel closes.
                on_request: Some(Arc::new(|_message, _control| {
                    Box::pin(std::future::pending::<Result<Value, String>>())
                })),
                ..ControlHandlers::default()
            },
        );
        let control = open_control(
            &sender_mux,
            ControlHandlers::default(),
            Duration::from_secs(5),
        )
        .await
        .expect("channel opens");
        let waiting = {
            let control = control.clone();
            tokio::spawn(async move {
                control
                    .call("cmd", Map::new(), Duration::from_secs(30))
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        control.close();
        let error = waiting.await.expect("joined").expect_err("closed");
        assert_eq!(error.message, "Receiver connection closed");
    }

    #[tokio::test]
    async fn state_events_reach_the_sender() {
        let (sender_mux, receiver_mux) = pair_of_muxes();
        let serve = serve_control(
            &receiver_mux,
            ControlHandlers {
                on_open: Some(Arc::new(|control| {
                    control.push(json!({ "event": "state", "state": { "status": "playing" } }));
                })),
                ..ControlHandlers::default()
            },
        );
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let control = open_control(
            &sender_mux,
            ControlHandlers {
                on_event: Some(Arc::new(move |message| {
                    let _ = tx.send(message);
                })),
                ..ControlHandlers::default()
            },
            Duration::from_secs(5),
        )
        .await
        .expect("channel opens");
        let message = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("event arrives")
            .expect("channel open");
        assert_eq!(message["event"], "state");
        assert_eq!(message["state"]["status"], "playing");
        control.close();
        drop(serve);
    }
}
