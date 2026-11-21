use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use bytes::Bytes;
use compact_encoding::{decode, encode};
use peartube_native::control_channel::{
    Control, ControlHandlers, ServeHandle, Spec, open_channel, serve_channel,
};
use peartube_native::control_protocol::{Body, CAST, GUARD, MAX_UINT, Protocol, SEARCH, Struct};
use peartube_native::wire::peartube::*;
use protomux::{Channel, ChannelOptions, Event, FrameRead, Mux, StreamInfo};
use tokio::sync::{mpsc, watch};
use tokio::time::timeout;

const WAIT: Duration = Duration::from_secs(5);
static GUARD_SPEC: Spec = Spec {
    protocol: &GUARD,
    label: "Guard",
    refused: "refused",
    duplicate: "duplicate",
    timed_out: "timeout",
};
static CAST_SPEC: Spec = Spec {
    protocol: &CAST,
    ..GUARD_SPEC
};
static SEARCH_SPEC: Spec = Spec {
    protocol: &SEARCH,
    ..GUARD_SPEC
};

fn muxes() -> (Mux, Mux) {
    let ((ar, aw), (br, bw)) = protomux::memory::duplex(64);
    let info = |initiator| StreamInfo {
        is_initiator: initiator,
        remote_public_key: Some(if initiator { [2; 32] } else { [1; 32] }),
        handshake_hash: Some([7; 64]),
    };
    (Mux::new(ar, aw, info(true)), Mux::new(br, bw, info(false)))
}
fn frame(kind: u64, op: u64, id: u64, value: Bytes) -> Frame {
    Frame {
        kind,
        op,
        id,
        value,
    }
}
fn hello() -> GuardHello {
    GuardHello {
        role: "endpoint".into(),
    }
}
fn ack() -> GuardAck {
    GuardAck { status: 1 }
}
fn guard_handlers(calls: Arc<AtomicUsize>) -> ControlHandlers {
    ControlHandlers {
        handled: (1 << 0) | (1 << 1),
        on_request: Some(Arc::new(move |_, body, _| {
            calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                if let Body::GuardHello(v) = body {
                    match v.role.as_str() {
                        "panic" => panic!("handler panic"),
                        "late-panic" => {
                            tokio::task::yield_now().await;
                            panic!("late panic")
                        }
                        "bad-result" => return Ok(hello().into()),
                        "error" => return Err("é".repeat(270)),
                        _ => (),
                    }
                }
                Ok(ack().into())
            })
        })),
        ..Default::default()
    }
}
async fn raw_pair(
    a: &Mux,
    b: &Mux,
    spec: &'static Spec,
    mut handlers: ControlHandlers,
) -> (Channel, Control, ServeHandle) {
    let (tx, mut rx) = mpsc::unbounded_channel();
    handlers.on_open = Some(Arc::new(move |c| {
        tx.send(c).unwrap();
    }));
    let service = serve_channel(b, spec, handlers);
    let mut raw = a
        .open_channel(ChannelOptions::new(spec.protocol.name))
        .unwrap();
    assert!(matches!(
        timeout(WAIT, raw.recv()).await.unwrap(),
        Some(Event::Opened { .. })
    ));
    let c = timeout(WAIT, rx.recv()).await.unwrap().unwrap();
    (raw, c, service)
}
async fn reply(ch: &mut Channel) -> Frame {
    let Some(Event::Message(m)) = timeout(WAIT, ch.recv()).await.expect("frame received") else {
        panic!("channel closed")
    };
    assert_eq!(m.kind, 0);
    decode(m.data).unwrap()
}
async fn healthy(raw: &mut Channel, id: u64) {
    raw.sender().send(0, &frame(0, 0, id, encode(&hello())));
    assert_eq!(reply(raw).await, frame(1, 0, id, encode(&ack())));
}
/// A valid `hello` request frame of exactly `bytes` bytes: one byte each for kind, op and id,
/// three for the value's length, and the value padded with bytes its decoder ignores.
fn sized(id: u64, bytes: usize) -> Bytes {
    let mut value = encode(&hello()).to_vec();
    value.resize(bytes - 6, 0);
    let out = encode(&frame(0, 0, id, value.into()));
    assert_eq!(out.len(), bytes);
    out
}

// Every valid probe must receive its own reply AND a new block from the existing connection;
// a stale cached block or a lucky check of !is_destroyed() cannot make the test pass.
#[tokio::test]
async fn malformed_requests_leave_valid_requests_and_block_reads_working() {
    let (a, b) = muxes();
    let wd = tempfile::tempdir().unwrap();
    let rd = tempfile::tempdir().unwrap();
    let ws = corestore::Corestore::open(wd.path()).unwrap();
    let rs = corestore::Corestore::open(rd.path()).unwrap();
    let w = ws
        .get(corestore::GetOptions {
            key_pair: Some(hypercore::crypto::KeyPair::from_seed(&[3; 32])),
            ..Default::default()
        })
        .await
        .unwrap();
    let r = rs.get(corestore::GetOptions::key(*w.key())).await.unwrap();
    let _active = r.core().session(true);
    w.core().replicate(&b);
    r.core().replicate(&a);
    let calls = Arc::new(AtomicUsize::new(0));
    let (mut raw, _, _serve) = raw_pair(&a, &b, &GUARD_SPEC, guard_handlers(calls.clone())).await;
    let mut block_index = 0;
    macro_rules! download {
        () => {{
            let block = vec![block_index as u8; 256];
            w.append(&[block]).await.unwrap();
            assert_eq!(r.core().count_local(block_index, block_index + 1), 0);
            timeout(
                WAIT,
                r.core().download(block_index, Some(block_index + 1), true),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(r.core().count_local(block_index, block_index + 1), 1);
            block_index += 1;
        }};
    }
    // A valid request of exactly 64 KiB is answered; one byte more is dropped as a bad frame.
    raw.sender().send_raw(0, &sized(9, 65_536));
    assert_eq!(reply(&mut raw).await, frame(1, 0, 9, encode(&ack())));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    // Skips and unsafe frame headers must never turn into a response or dispatch a handler.
    let big = MAX_UINT + 1;
    let invalid = [
        sized(10, 65_537),
        encode(&frame(0, 0, big, Bytes::new())),
        encode(&frame(0, big, 3, Bytes::new())),
        encode(&frame(big, 0, 3, Bytes::new())),
        encode(&frame(4, 0, 1, Bytes::new())),
        encode(&frame(0, 0, 0, encode(&hello()))),
        encode(&frame(3, 11, 1, Bytes::new())),
        encode(&frame(1, 0, 4, Bytes::new())),
    ];
    for (i, bytes) in invalid.into_iter().enumerate() {
        raw.sender().send_raw(0, &bytes);
        healthy(&mut raw, 100 + i as u64).await;
        download!();
        assert_eq!(calls.load(Ordering::SeqCst), i + 2);
    }
    let role = |role: &str| encode(&GuardHello { role: role.into() });
    // The envelope's own errors carry the app's empty message; a handler's travels cut to 256.
    let cut = "é".repeat(256);
    for (op, value, code, message) in [
        (99, Bytes::new(), "UNKNOWN_OP", ""),
        (11, Bytes::new(), "UNKNOWN_OP", ""),
        (2, Bytes::new(), "UNKNOWN_OP", ""),
        (1, Bytes::new(), "DECODE_ERROR", ""),
        (0, role("panic"), "HANDLER_ERROR", "handler panic"),
        (0, role("late-panic"), "HANDLER_ERROR", "late panic"),
        (0, role("bad-result"), "ENCODE_ERROR", ""),
        (0, role("error"), "HANDLER_ERROR", cut.as_str()),
    ] {
        raw.sender().send(0, &frame(0, op, 3, value));
        let f = timeout(Duration::from_secs(1), reply(&mut raw))
            .await
            .expect("immediate error");
        assert_eq!((f.kind, f.op, f.id), (2, op, 3));
        let err: RemoteError = decode(f.value).unwrap();
        assert_eq!((err.code.as_str(), err.message.as_str()), (code, message));
        healthy(&mut raw, 7).await;
        download!();
    }
    // Six invalid frame headers and one bad value above. The eighth is answered DECODE_ERROR,
    // then closes only this channel.
    raw.sender().send(0, &frame(0, 1, 3, Bytes::new()));
    let f = reply(&mut raw).await;
    assert_eq!((f.kind, f.op, f.id), (2, 1, 3));
    assert_eq!(decode::<RemoteError>(f.value).unwrap().code, "DECODE_ERROR");
    assert!(matches!(
        timeout(WAIT, raw.recv()).await.unwrap(),
        Some(Event::Closed { .. })
    ));
    download!();
    assert_eq!(block_index, 17);
    assert!(!a.is_destroyed() && !b.is_destroyed());
}

async fn answer_case(
    c: &Control,
    raw: &mut Channel,
    op: u64,
    input: Body,
    value: Bytes,
    kind: u64,
) -> Result<Body, peartube_native::control_channel::ControlError> {
    let c = c.clone();
    let call = tokio::spawn(async move { c.call(op, input, WAIT).await });
    let f = reply(raw).await;
    assert_eq!((f.kind, f.op), (0, op));
    raw.sender().send(0, &frame(kind, op, f.id, value));
    call.await.unwrap()
}
#[tokio::test]
async fn unsafe_flags_numbers_and_all_three_counts_spend_one_budget_unit() {
    let (a, b) = muxes();
    let bad_flags = encode(&(MAX_UINT + 1));
    let bad_hits = encode(&SearchHits {
        hits: vec![SearchHit { uri: String::new() }; 21],
    });
    let bad_cards = encode(&GuardCards {
        cards: vec![Bytes::new(); 65],
    });
    let mut state =
        peartube_native::cast::control::state_to_wire(&peartube_native::cast::state::idle(0));
    state.tracks = vec![
        CastTrack {
            stream: 0,
            kind: String::new(),
            codec: String::new(),
            language: String::new(),
            title: String::new()
        };
        129
    ];
    let bad_state = encode(&state);
    state.tracks.clear();
    let flagged = |bytes: Bytes| {
        let mut out = bad_flags.to_vec();
        out.extend_from_slice(&bytes[1..]);
        Bytes::from(out)
    };
    assert!(
        Body::from(SearchHits {
            hits: vec![SearchHit { uri: String::new() }; 21]
        })
        .encode()
        .is_none()
    );
    for (spec, op, req, response, bad) in [
        (
            &SEARCH_SPEC,
            0,
            Body::from(SearchQuery {
                q: "ab".into(),
                mid: "".into(),
                limit: 20,
            }),
            Struct::SearchHits,
            bad_hits,
        ),
        (
            &GUARD_SPEC,
            6,
            Body::from(GuardKey { key: [0; 32] }),
            Struct::GuardCards,
            bad_cards,
        ),
        (
            &CAST_SPEC,
            1,
            Body::from(CastSubscribe { token: "".into() }),
            Struct::CastState,
            bad_state,
        ),
        (
            &GUARD_SPEC,
            3,
            Body::from(GuardKey { key: [0; 32] }),
            Struct::GuardGetResult,
            bad_flags.clone(),
        ),
        (
            &CAST_SPEC,
            1,
            Body::from(CastSubscribe { token: "".into() }),
            Struct::CastState,
            flagged(encode(&state)),
        ),
        (
            &GUARD_SPEC,
            0,
            Body::from(hello()),
            Struct::GuardAck,
            encode(&(MAX_UINT + 1)),
        ),
    ] {
        let (mut raw, c, _serve) = raw_pair(&a, &b, spec, ControlHandlers::default()).await;
        assert!(response.decode(bad.clone()).is_none());
        for _ in 0..7 {
            let error = answer_case(&c, &mut raw, op, req.clone(), bad.clone(), 1)
                .await
                .unwrap_err();
            assert_eq!(
                (error.code.as_deref(), error.message.as_str()),
                (
                    Some("DECODE_ERROR"),
                    "Guard sent an answer that does not decode"
                )
            );
            assert!(!c.is_closed());
        }
        // Unmatched response with same id and wrong op must not resolve/decode or consume the 8th unit.
        let c2 = c.clone();
        let call = tokio::spawn(async move { c2.call(op, req, WAIT).await });
        let f = reply(&mut raw).await;
        raw.sender().send(0, &frame(1, 99, f.id, Bytes::new()));
        raw.sender().send(
            0,
            &frame(
                2,
                op,
                f.id,
                encode(&RemoteError {
                    code: "HANDLER_ERROR".into(),
                    message: "busy".into(),
                }),
            ),
        );
        let e = call.await.unwrap().unwrap_err();
        assert_eq!(e.code.as_deref(), Some("HANDLER_ERROR"));
        assert_eq!(e.message, "busy");
        raw.sender().send_raw(0, &[]);
        timeout(WAIT, c.closed()).await.unwrap();
        assert!(!a.is_destroyed() && !b.is_destroyed());
    }
    let cmd = peartube_native::cast::control::command_to_wire(
        &serde_json::json!({"action":"play"}),
        String::new(),
    );
    assert!(Struct::CastCommand.decode(flagged(encode(&cmd))).is_none());
}

#[tokio::test]
async fn event_result_distinguishes_rejection_from_a_queued_frame() {
    use peartube_native::control_channel::PushResult::*;
    let (a, b) = muxes();
    let (mut raw, c, _serve) = raw_pair(&a, &b, &CAST_SPEC, ControlHandlers::default()).await;
    let state = peartube_native::cast::control::state_to_wire(&serde_json::Map::new());
    let mut huge = state.clone();
    huge.error = "x".repeat(65536);
    assert_eq!(c.push(3, huge.into()), NotSent);
    assert_eq!(c.push(99, state.clone().into()), NotSent);
    assert_eq!(c.push(3, state.clone().into()), Sent);
    assert_eq!(reply(&mut raw).await, frame(3, 3, 0, encode(&state)));
    let _remote = a.open_channel(ChannelOptions::new("event-load")).unwrap();
    let load = b.open_channel(ChannelOptions::new("event-load")).unwrap();
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    assert!(
        load.sender()
            .send_raw(0, &vec![0; protomux::OUTBOUND_HIGH_WATER - 8])
    );
    let mut queued = state.clone();
    queued.updated_at = 1;
    assert_eq!(c.push(3, queued.clone().into()), WaitForDrain);
    assert_eq!(reply(&mut raw).await, frame(3, 3, 0, encode(&queued)));
    timeout(WAIT, c.drained()).await.unwrap();
    assert_eq!(c.push(3, state.clone().into()), Sent);
    assert_eq!(reply(&mut raw).await, frame(3, 3, 0, encode(&state)));
}

#[tokio::test]
async fn bidirectional_calls_share_an_id_and_bad_local_encodes_send_nothing() {
    let (a, b) = muxes();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut h = guard_handlers(calls.clone());
    let (tx, mut rx) = mpsc::unbounded_channel();
    h.on_open = Some(Arc::new(move |c| {
        tx.send(c).unwrap();
    }));
    let _serve = serve_channel(&b, &GUARD_SPEC, h);
    let ac = open_channel(&a, &GUARD_SPEC, guard_handlers(calls.clone()), WAIT)
        .await
        .unwrap();
    let bc = rx.recv().await.unwrap();
    let (x, y) = tokio::join!(
        ac.call(0, hello().into(), WAIT),
        bc.call(0, hello().into(), WAIT)
    );
    assert_eq!(x.unwrap(), Body::from(ack()));
    assert_eq!(y.unwrap(), Body::from(ack()));
    assert!(
        ac.call(
            0,
            GuardHello {
                role: "x".repeat(65536)
            }
            .into(),
            WAIT
        )
        .await
        .is_err()
    );
    assert_eq!(
        ac.push(
            11,
            GuardCircuitDead {
                udx_id: MAX_UINT + 1
            }
            .into()
        ),
        peartube_native::control_channel::PushResult::NotSent
    );
    assert_eq!(
        ac.call(0, hello().into(), WAIT).await.unwrap(),
        Body::from(ack())
    );
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn queued_call_is_delivered_exactly_once() {
    let (a, b) = muxes();
    let calls = Arc::new(AtomicUsize::new(0));
    let _serve = serve_channel(&b, &GUARD_SPEC, guard_handlers(calls.clone()));
    let ac = open_channel(&a, &GUARD_SPEC, ControlHandlers::default(), WAIT)
        .await
        .unwrap();
    let _remote = b.open_channel(ChannelOptions::new("queue-load")).unwrap();
    let load = a.open_channel(ChannelOptions::new("queue-load")).unwrap();
    // A current-thread runtime cannot run the writer between this send and the synchronous first
    // poll of call(). Leave less room than HELLO needs: that very request crosses high water.
    tokio::task::yield_now().await;
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    assert!(a.is_drained());
    let size = protomux::OUTBOUND_HIGH_WATER - 8;
    assert!(
        load.sender().send_raw(0, &vec![0; size]),
        "queue is still below high water"
    );
    let mut call = Box::pin(ac.call(0, hello().into(), WAIT));
    assert!(futures_util::poll!(&mut call).is_pending());
    assert!(!a.is_drained(), "request itself crossed high water");
    assert_eq!(call.await.unwrap(), Body::from(ack()));
    // Following valid request is a barrier behind any duplicate already queued on this stream.
    assert_eq!(
        ac.call(0, hello().into(), WAIT).await.unwrap(),
        Body::from(ack())
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn events_require_matching_kind_id_and_handler_and_pending_calls_close() {
    let (a, b) = muxes();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let h = ControlHandlers {
        handled: 1 << 11,
        on_event: Some(Arc::new(move |_, v, _| {
            tx.send(v).unwrap();
        })),
        ..Default::default()
    };
    let (mut raw, c, _serve) = raw_pair(&a, &b, &GUARD_SPEC, h).await;
    let dead = GuardCircuitDead { udx_id: 42 };
    raw.sender().send(0, &frame(3, 11, 1, encode(&dead)));
    raw.sender().send(0, &frame(3, 0, 0, Bytes::new()));
    raw.sender().send(0, &frame(3, 5, 0, Bytes::new()));
    raw.sender().send(0, &frame(3, 11, 0, encode(&dead)));
    assert_eq!(timeout(WAIT, rx.recv()).await.unwrap(), Some(dead.into()));
    let c2 = c.clone();
    let pending = tokio::spawn(async move { c2.call(0, hello().into(), WAIT).await });
    let ask = reply(&mut raw).await;
    assert_eq!(ask.id, 1);
    for _ in 0..8 {
        raw.sender().send(0, &frame(3, 11, 0, Bytes::new()));
    }
    assert_eq!(
        pending.await.unwrap().unwrap_err().message,
        "Guard connection closed"
    );
    assert!(
        rx.try_recv().is_err(),
        "only the valid event reached its handler"
    );
    assert!(!a.is_destroyed() && !b.is_destroyed());
}

// An open whose connection goes away before the other side answers fails with the closed
// connection, not with the version refusal: only a peer that answers the open with a refusal,
// on a live connection, is "another version" (that case stays `refused`, see the next test).
#[tokio::test]
async fn an_open_on_a_connection_that_closes_is_not_a_refusal() {
    let (a, b) = muxes();
    let mut pairs = b.pair(GUARD.name, None);
    b.ready();
    let opening = tokio::spawn({
        let a = a.clone();
        async move { open_channel(&a, &GUARD_SPEC, ControlHandlers::default(), WAIT).await }
    });
    // The other side has the open and has not answered it when its connection goes.
    let held = timeout(WAIT, pairs.recv()).await.unwrap().unwrap();
    b.destroy();
    let error = timeout(WAIT, opening).await.unwrap().unwrap().unwrap_err();
    assert_eq!(
        (error.code.as_deref(), error.message.as_str()),
        (None, "Guard connection closed")
    );
    drop(held);

    // The cast controller's open: a lost connection is "Receiver connection closed" (which
    // `startSession` retries once), never the version message.
    use peartube_native::cast::control::{VERSION_ERROR, open_control};
    let (a, b) = muxes();
    let mut pairs = b.pair(CAST.name, None);
    b.ready();
    let opening = tokio::spawn({
        let a = a.clone();
        async move { open_control(&a, ControlHandlers::default(), WAIT).await }
    });
    let held = timeout(WAIT, pairs.recv()).await.unwrap().unwrap();
    b.destroy();
    let error = timeout(WAIT, opening).await.unwrap().unwrap().unwrap_err();
    assert_ne!(error.message, VERSION_ERROR);
    assert_eq!(error.message, "Receiver connection closed");
    drop(held);
}

#[tokio::test]
async fn old_protocol_names_are_never_negotiated() {
    for (spec, old) in [
        (&SEARCH_SPEC, "peartube/search/v1"),
        (&CAST_SPEC, "peartube-cast/v1"),
        (&GUARD_SPEC, "peartube/private-guard/v1"),
    ] {
        let (a, b) = muxes();
        let mut old_pairs = b.pair(old, None);
        b.ready();
        let err = open_channel(&a, spec, ControlHandlers::default(), WAIT)
            .await
            .unwrap_err();
        assert_eq!(err.message, "refused");
        assert!(old_pairs.try_recv().is_err());
        assert!(!a.is_destroyed() && !b.is_destroyed());
        let _serve = serve_channel(&b, spec, ControlHandlers::default());
        let mut old_channel = a.open_channel(ChannelOptions::new(old)).unwrap();
        // The old side cannot negotiate a v2 channel with a new-only server.
        b.unpair(old, None);
        drop(old_pairs);
        assert!(matches!(
            timeout(WAIT, old_channel.recv()).await.unwrap(),
            Some(Event::Closed { .. })
        ));
    }
}

// Finite floats on both sides: `volume` of `cast-command` and `cast-state` that is NaN or
// infinite is an encode failure when sent and a decode failure (a strike) when received.
#[tokio::test]
async fn non_finite_volumes_neither_leave_nor_arrive() {
    use peartube_native::cast::control::{command_to_wire, state_to_wire};
    use peartube_native::control_channel::PushResult::NotSent;
    let (a, b) = muxes();
    let commands = Arc::new(AtomicUsize::new(0));
    let seen = commands.clone();
    let idle = state_to_wire(&serde_json::Map::new());
    let answer = idle.clone();
    let events = Arc::new(AtomicUsize::new(0));
    let handlers = ControlHandlers {
        handled: (1 << 2) | (1 << 3),
        on_request: Some(Arc::new(move |_, _, _| {
            seen.fetch_add(1, Ordering::SeqCst);
            let answer = answer.clone();
            Box::pin(async move { Ok(answer.into()) })
        })),
        on_event: Some({
            let events = events.clone();
            Arc::new(move |_, _, _| {
                events.fetch_add(1, Ordering::SeqCst);
            })
        }),
        ..Default::default()
    };
    let (mut raw, c, _serve) = raw_pair(&a, &b, &CAST_SPEC, handlers).await;
    let command = command_to_wire(
        &serde_json::json!({ "action": "volume", "volume": 0.5 }),
        "t".into(),
    );
    for volume in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        // Sent: nothing leaves.
        let mut state = idle.clone();
        state.volume = volume;
        assert_eq!(c.push(3, state.clone().into()), NotSent);
        let mut bad = command.clone();
        bad.volume = volume;
        // Received: the request is answered DECODE_ERROR and never reaches the handler.
        raw.sender().send(0, &frame(0, 2, 5, encode(&bad)));
        let f = reply(&mut raw).await;
        assert_eq!((f.kind, f.op, f.id), (2, 2, 5));
        assert_eq!(decode::<RemoteError>(f.value).unwrap().code, "DECODE_ERROR");
        // Received as an event: dropped before the handler.
        raw.sender().send(0, &frame(3, 3, 0, encode(&state)));
    }
    raw.sender().send(0, &frame(3, 3, 0, encode(&idle)));
    assert_eq!(commands.load(Ordering::SeqCst), 0);
    raw.sender().send(0, &frame(0, 2, 6, encode(&command)));
    assert_eq!(reply(&mut raw).await.kind, 1);
    assert_eq!(commands.load(Ordering::SeqCst), 1);
    assert_eq!(
        events.load(Ordering::SeqCst),
        1,
        "only the finite state arrived"
    );
}

// A peer cannot make a call fail with a local-looking code; an event on a closed channel is
// "not sent"; an open that times out says so, and is not reported as a refusal.
#[tokio::test]
async fn foreign_codes_closed_events_and_open_timeouts() {
    use peartube_native::control_channel::PushResult::NotSent;
    let (a, b) = muxes();
    let (mut raw, c, _serve) = raw_pair(&a, &b, &GUARD_SPEC, ControlHandlers::default()).await;
    let ask = {
        let c = c.clone();
        tokio::spawn(async move { c.call(0, hello().into(), WAIT).await })
    };
    let f = reply(&mut raw).await;
    let foreign = RemoteError {
        code: "ETIMEDOUT".into(),
        message: "made up".into(),
    };
    raw.sender().send(0, &frame(2, 0, f.id, encode(&foreign)));
    let error = ask.await.unwrap().unwrap_err();
    assert_eq!(
        (error.code.as_deref(), error.message.as_str()),
        (Some("HANDLER_ERROR"), "made up")
    );
    c.close();
    assert_eq!(c.push(11, GuardCircuitDead { udx_id: 1 }.into()), NotSent);

    // The other side never answers the open: no pairer, and it never declares itself ready.
    let (quiet, _held) = muxes();
    let error = open_channel(
        &quiet,
        &GUARD_SPEC,
        ControlHandlers::default(),
        Duration::from_millis(200),
    )
    .await
    .unwrap_err();
    assert_eq!(error.message, GUARD_SPEC.timed_out);
}

/// A reader that passes nothing on while its gate is shut, as when the peer stops reading: what
/// the other side writes then stays queued in its mux.
struct Gated {
    inner: protomux::memory::MemoryReader,
    open: watch::Receiver<bool>,
}

impl FrameRead for Gated {
    async fn read(&mut self) -> std::io::Result<Option<Bytes>> {
        let _ = self.open.wait_for(|open| *open).await;
        self.inner.read().await
    }
}

/// Like [`muxes`], with a gate on each side's reading: shutting `gate_b` holds what `a` sends,
/// shutting `gate_a` holds what `b` sends.
fn gated_muxes() -> (Mux, Mux, watch::Sender<bool>, watch::Sender<bool>) {
    let ((ar, aw), (br, bw)) = protomux::memory::duplex(64);
    let (gate_a, open_a) = watch::channel(true);
    let (gate_b, open_b) = watch::channel(true);
    let info = |initiator| StreamInfo {
        is_initiator: initiator,
        remote_public_key: Some(if initiator { [2; 32] } else { [1; 32] }),
        handshake_hash: Some([7; 64]),
    };
    let a = Mux::new(
        Gated {
            inner: ar,
            open: open_a,
        },
        aw,
        info(true),
    );
    let b = Mux::new(
        Gated {
            inner: br,
            open: open_b,
        },
        bw,
        info(false),
    );
    (a, b, gate_a, gate_b)
}

/// An opened side channel from `from` to `to` to load `from`'s output with.
async fn load_channel(from: &Mux, to: &Mux) -> (Channel, Channel) {
    let mut remote = to.open_channel(ChannelOptions::new("load")).unwrap();
    let mut load = from.open_channel(ChannelOptions::new("load")).unwrap();
    assert!(timeout(WAIT, load.opened()).await.unwrap().is_some());
    assert!(timeout(WAIT, remote.opened()).await.unwrap().is_some());
    (load, remote)
}

/// With the peer's reading shut, puts `from` above its high-water mark: the transport takes 64
/// frames, the rest stays queued.
async fn hold_output(from: &Mux, load: &Channel) {
    for _ in 0..70 {
        load.send_raw(0, &[0; 16]);
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    load.send_raw(0, &vec![0; protomux::OUTBOUND_HIGH_WATER]);
    assert!(!from.is_drained(), "the output is held");
}

// Amended section 3: the paced producer waits for a real drain, or the channel closing. Elapsed
// time is not a drain, and a close releases the waiter at once.
#[tokio::test]
async fn drained_waits_for_a_real_drain_or_the_close() {
    let (a, b, _gate_a, gate_b) = gated_muxes();
    let _serve = serve_channel(&b, &GUARD_SPEC, ControlHandlers::default());
    let c = open_channel(&a, &GUARD_SPEC, ControlHandlers::default(), WAIT)
        .await
        .unwrap();
    let (load, _remote) = load_channel(&a, &b).await;
    let wait = |c: &Control| {
        let c = c.clone();
        tokio::spawn(async move { c.drained().await })
    };

    gate_b.send_replace(false);
    hold_output(&a, &load).await;
    let waiting = wait(&c);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!waiting.is_finished(), "nothing drained yet");
    gate_b.send_replace(true);
    timeout(WAIT, waiting).await.expect("a real drain").unwrap();

    gate_b.send_replace(false);
    hold_output(&a, &load).await;
    let waiting = wait(&c);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!waiting.is_finished(), "nothing drained yet");
    c.close();
    timeout(Duration::from_secs(1), waiting)
        .await
        .expect("the close releases the waiter")
        .unwrap();
}

// The reader never waits for the transport: with the answering side's output held, its
// UNKNOWN_OP and DECODE_ERROR replies queue, and the next request's handler still starts.
#[tokio::test]
async fn held_replies_never_stall_the_next_request() {
    let (a, b, gate_a, _gate_b) = gated_muxes();
    let started = Arc::new(AtomicUsize::new(0));
    let handlers = ControlHandlers {
        handled: 1 << 0,
        on_request: Some({
            let started = started.clone();
            Arc::new(move |_, _, _| {
                started.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(ack().into()) })
            })
        }),
        ..Default::default()
    };
    let (mut raw, _c, _serve) = raw_pair(&a, &b, &GUARD_SPEC, handlers).await;
    let (load, _remote) = load_channel(&b, &a).await;
    gate_a.send_replace(false);
    hold_output(&b, &load).await;
    raw.sender().send(0, &frame(0, 99, 1, Bytes::new()));
    raw.sender()
        .send(0, &frame(0, 0, 2, Bytes::from_static(&[0xff])));
    raw.sender().send(0, &frame(0, 0, 3, encode(&hello())));
    timeout(Duration::from_secs(1), async {
        while started.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the handler started while the replies were held");
    assert!(!b.is_drained(), "still held");
    gate_a.send_replace(true);
    let codes: Vec<(u64, u64, String)> = [reply(&mut raw).await, reply(&mut raw).await]
        .into_iter()
        .map(|f| (f.kind, f.id, decode::<RemoteError>(f.value).unwrap().code))
        .collect();
    assert_eq!(
        codes,
        [(2, 1, "UNKNOWN_OP".into()), (2, 2, "DECODE_ERROR".into())]
    );
    assert_eq!(reply(&mut raw).await, frame(1, 0, 3, encode(&ack())));
}

// Ids are per side and count from 1. A request that fails here (a value or a frame too large, a
// value of the wrong struct) is ENCODE_ERROR, sends nothing and uses no id; a call on a closed
// channel fails with the close first; an unanswered call is ETIMEDOUT.
#[tokio::test]
async fn request_ids_start_at_one_on_each_side_and_local_failures_send_nothing() {
    for opener in [true, false] {
        let (a, b) = muxes();
        let mut held = None;
        let (mut raw, c) = if opener {
            let mut pairs = b.pair(GUARD.name, None);
            b.ready();
            let opening = tokio::spawn({
                let a = a.clone();
                async move { open_channel(&a, &GUARD_SPEC, ControlHandlers::default(), WAIT).await }
            });
            // The pair request stays held until this side's channel answers it.
            let request = timeout(WAIT, pairs.recv()).await.unwrap().unwrap();
            let mut raw = b.open_channel(ChannelOptions::new(GUARD.name)).unwrap();
            drop(request);
            assert!(timeout(WAIT, raw.opened()).await.unwrap().is_some());
            (raw, opening.await.unwrap().unwrap())
        } else {
            let (raw, c, serve) = raw_pair(&a, &b, &GUARD_SPEC, ControlHandlers::default()).await;
            held = Some(serve);
            (raw, c)
        };
        let role = |n: usize| GuardHello {
            role: "x".repeat(n),
        };
        // 70,000: the value alone is too large. 65,530: the value fits, the frame does not.
        for local in [role(70_000).into(), role(65_530).into(), Body::from(ack())] {
            let error = c.call(0, local, WAIT).await.unwrap_err();
            assert_eq!(error.code.as_deref(), Some("ENCODE_ERROR"), "{error:?}");
        }
        let mut first = Box::pin(c.call(0, hello().into(), WAIT));
        assert!(futures_util::poll!(first.as_mut()).is_pending());
        let mut second = Box::pin(c.call(0, hello().into(), WAIT));
        assert!(futures_util::poll!(second.as_mut()).is_pending());
        let sent = [reply(&mut raw).await, reply(&mut raw).await];
        assert_eq!(
            sent.iter()
                .map(|f| (f.kind, f.op, f.id))
                .collect::<Vec<_>>(),
            [(0, 0, 1), (0, 0, 2)],
            "opener: {opener}"
        );
        raw.sender()
            .send(0, &frame(1, 0, 2, encode(&GuardAck { status: 0 })));
        raw.sender().send(0, &frame(1, 0, 1, encode(&ack())));
        assert_eq!(first.await.unwrap(), Body::from(ack()));
        assert_eq!(second.await.unwrap(), Body::from(GuardAck { status: 0 }));
        let error = c
            .call(0, hello().into(), Duration::from_millis(100))
            .await
            .unwrap_err();
        assert_eq!(
            (error.code.as_deref(), error.message.as_str()),
            (Some("ETIMEDOUT"), "Guard hello timed out")
        );
        assert_eq!(reply(&mut raw).await.id, 3);
        c.close();
        let error = c.call(0, role(70_000).into(), WAIT).await.unwrap_err();
        assert_eq!(
            (error.code, error.message.as_str()),
            (None, "Guard connection closed")
        );
        drop(held);
    }
}

// A reply matches by op and id. One with the call's id under another known op, whose value would
// decode as the call's answer, settles nothing.
#[tokio::test]
async fn a_reply_under_another_known_op_leaves_the_call_pending() {
    let (a, b) = muxes();
    let (mut raw, c, _serve) = raw_pair(&a, &b, &GUARD_SPEC, ControlHandlers::default()).await;
    let mut call = Box::pin(c.call(6, GuardKey { key: [0; 32] }.into(), WAIT));
    assert!(futures_util::poll!(call.as_mut()).is_pending());
    let ask = reply(&mut raw).await;
    assert_eq!((ask.kind, ask.op), (0, 6));
    let cards = |card: &'static [u8]| GuardCards {
        cards: vec![Bytes::from_static(card)],
    };
    raw.sender()
        .send(0, &frame(1, 7, ask.id, encode(&cards(b"WRONG OP"))));
    let foreign = RemoteError {
        code: "HANDLER_ERROR".into(),
        message: "WRONG OP".into(),
    };
    raw.sender().send(0, &frame(2, 7, ask.id, encode(&foreign)));
    // A request behind them on the wire: its answer means both were handled.
    raw.sender().send(0, &frame(0, 0, 9, encode(&hello())));
    assert_eq!(reply(&mut raw).await.id, 9);
    assert!(futures_util::poll!(call.as_mut()).is_pending());
    raw.sender()
        .send(0, &frame(1, 6, ask.id, encode(&cards(b"right op"))));
    assert_eq!(call.await.unwrap(), Body::from(cards(b"right op")));
}

// Strikes inside the window add up; one older than the window no longer counts. The exact edge
// (10,000 ms still counts) is the unit test next to the window.
#[tokio::test]
async fn strikes_older_than_the_window_are_forgotten() {
    let (a, b) = muxes();
    let calls = Arc::new(AtomicUsize::new(0));
    let (mut raw, c, _serve) = raw_pair(&a, &b, &GUARD_SPEC, guard_handlers(calls)).await;
    for _ in 0..7 {
        raw.sender().send_raw(0, &[0xff]);
    }
    healthy(&mut raw, 1).await;
    tokio::time::sleep(Duration::from_millis(10_200)).await;
    for _ in 0..7 {
        raw.sender().send_raw(0, &[0xff]);
    }
    healthy(&mut raw, 2).await;
    assert!(!c.is_closed(), "the first seven expired");
    raw.sender().send_raw(0, &[0xff]);
    timeout(WAIT, c.closed())
        .await
        .expect("eight in the window");
}

/// One protocol per race trial, so every trial has its own channel opened before the output is
/// held.
const RACE_TRIALS: usize = 2000;
static RACE_NAMES: LazyLock<Vec<String>> =
    LazyLock::new(|| (0..RACE_TRIALS).map(|i| format!("race/{i}")).collect());
static RACE_PROTOCOLS: LazyLock<Vec<Protocol>> = LazyLock::new(|| {
    RACE_NAMES
        .iter()
        .map(|name| Protocol {
            name,
            ops: GUARD.ops,
        })
        .collect()
});
static RACE_SPECS: LazyLock<Vec<Spec>> = LazyLock::new(|| {
    RACE_PROTOCOLS
        .iter()
        .map(|protocol| Spec {
            protocol,
            ..GUARD_SPEC
        })
        .collect()
});

// "Not sent" is decided before the send. A frame the mux accepted is "sent" or "sent, wait for
// drain", even when the channel closes right after; a close racing the push is run many times.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_accepted_event_is_never_reported_not_sent() {
    let (a, b, gate_a, _gate_b) = gated_muxes();
    let mut trials = Vec::new();
    for spec in RACE_SPECS.iter() {
        trials.push(raw_pair(&a, &b, spec, ControlHandlers::default()).await);
    }
    let (load, _remote) = load_channel(&b, &a).await;
    gate_a.send_replace(false);
    hold_output(&b, &load).await;
    let controls: Vec<Control> = trials.iter().map(|(_, c, _)| c.clone()).collect();
    let results = tokio::task::spawn_blocking(move || {
        controls
            .into_iter()
            .enumerate()
            .map(|(i, c)| {
                // Both threads spin, so the close lands at a spread of points inside the push.
                let flags = Arc::new((AtomicBool::new(false), AtomicBool::new(false)));
                let closer = {
                    let (c, flags) = (c.clone(), flags.clone());
                    std::thread::spawn(move || {
                        flags.0.store(true, Ordering::SeqCst);
                        while !flags.1.load(Ordering::SeqCst) {
                            std::hint::spin_loop();
                        }
                        for _ in 0..(i * 37) % 3000 {
                            std::hint::spin_loop();
                        }
                        c.close();
                    })
                };
                while !flags.0.load(Ordering::SeqCst) {
                    std::hint::spin_loop();
                }
                flags.1.store(true, Ordering::SeqCst);
                let result = c.push(11, GuardCircuitDead { udx_id: i as u64 }.into());
                closer.join().unwrap();
                result
            })
            .collect::<Vec<_>>()
    })
    .await
    .unwrap();
    gate_a.send_replace(true);
    let mut outcomes = std::collections::BTreeMap::new();
    for ((mut raw, _, _), result) in trials.into_iter().zip(results) {
        let mut delivered = false;
        while let Some(Event::Message(m)) = timeout(WAIT, raw.recv()).await.unwrap() {
            delivered |= decode::<Frame>(m.data).unwrap().kind == 3;
        }
        *outcomes
            .entry((format!("{result:?}"), delivered))
            .or_insert(0) += 1;
    }
    eprintln!("push outcome, frame delivered: {outcomes:?}");
    assert_eq!(
        outcomes.get(&("NotSent".into(), true)),
        None,
        "frames that left were reported not sent"
    );
}
