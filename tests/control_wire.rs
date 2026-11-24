use bytes::Bytes;
use compact_encoding::Codec;
use peartube_native::control_protocol::{CAST, GUARD, SEARCH};
use peartube_native::wire::peartube::*;
use serde_json::{Value, json};
use std::fmt::Debug;

fn check<T: Codec + PartialEq + Debug>(row: &Value, name: &str, expected: &T) {
    assert_eq!(row["name"], name);
    let hex = row["hex"].as_str().unwrap();
    let bytes = Bytes::from(
        hex.as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect::<Vec<_>>(),
    );
    assert_eq!(
        &compact_encoding::decode::<T>(bytes.clone()).unwrap(),
        expected,
        "JS -> Rust: {name}"
    );
    assert_eq!(
        compact_encoding::encode(expected),
        bytes,
        "Rust -> JS: {name}"
    );
}

#[test]
fn app_wire_bytes_in_both_directions() {
    let fixture: Value =
        serde_json::from_str(include_str!("../fixtures/control-wire-reference.json")).unwrap();
    let mut rows = fixture["cases"].as_array().unwrap().iter();
    macro_rules! case {
        ($name:literal, $value:expr) => {
            check(rows.next().unwrap(), $name, &$value);
        };
    }
    let max = (1u64 << 53) - 1;
    let key = std::array::from_fn(|i| i as u8);
    let slot = std::array::from_fn(|i| i as u8);
    let signature = std::array::from_fn(|i| i as u8);
    let x = Bytes::from_static(b"x");
    let err = RemoteError {
        code: "HANDLER_ERROR".into(),
        message: "bad".into(),
    };
    let entry = CastEntry {
        key: "k".into(),
        id: "i".into(),
        title: "t".into(),
        size: max,
        sha256: "h".into(),
    };
    let track = CastTrack {
        stream: -1,
        kind: "audio".into(),
        codec: "a".into(),
        language: "".into(),
        title: "".into(),
    };
    let command = CastCommand {
        token: "".into(),
        action: "volume".into(),
        entry: None,
        magnet_url: "".into(),
        start_position_ms: 0,
        position_ms: 0,
        delta_ms: -1,
        stream: -1,
        volume: 0.5,
        muted: -1,
    };
    let idle = CastState {
        tracks: vec![],
        status: "idle".into(),
        mode: "".into(),
        entry: None,
        position_ms: 0,
        duration_ms: 0,
        playing: false,
        buffering: false,
        ended: false,
        p2p_peers: 0,
        cached_bytes: 0,
        seeding: false,
        volume: 1.,
        muted: false,
        audio: -2,
        subtitle: -2,
        error: "".into(),
        updated_at: 0,
        video_decoder: "".into(),
        dropped_frames: -1,
    };
    case!("remote-error", err);
    case!(
        "search-query",
        SearchQuery {
            q: "".into(),
            mid: "".into(),
            limit: max
        }
    );
    case!("search-hit", SearchHit { uri: "x".into() });
    case!("search-hits", SearchHits { hits: vec![] });
    case!(
        "search-hits",
        SearchHits {
            hits: vec![SearchHit { uri: "x".into() }]
        }
    );
    case!("cast-pair", CastPair { code: "c".into() });
    case!(
        "cast-paired",
        CastPaired {
            device_id: "d".into(),
            name: "n".into(),
            public_key: key,
            token: "".into()
        }
    );
    case!("cast-subscribe", CastSubscribe { token: "".into() });
    case!("cast-entry", entry);
    case!("cast-track", track);
    case!("cast-command", command);
    case!(
        "cast-command",
        CastCommand {
            entry: Some(entry.clone()),
            delta_ms: -(1 << 52),
            stream: (1 << 52) - 1,
            ..command
        }
    );
    case!("cast-state", idle);
    case!(
        "cast-state",
        CastState {
            tracks: vec![track],
            entry: Some(entry),
            playing: true,
            buffering: true,
            ended: true,
            muted: true,
            seeding: true,
            volume: 0.5,
            updated_at: max,
            ..idle
        }
    );
    let hello = GuardHello { role: "hop".into() };
    let ack = GuardAck { status: 1 };
    case!("guard-hello", hello);
    case!("guard-ack", ack);
    case!(
        "guard-register",
        GuardRegister {
            route_token: key,
            ttl_ms: max
        }
    );
    case!("guard-key", GuardKey { key: [0; 32] });
    case!(
        "guard-put-mutable",
        GuardPutMutable {
            public_key: key,
            seq: max,
            signature,
            value: x.clone()
        }
    );
    let record = GuardRecord {
        seq: max,
        signature,
        value: x.clone(),
    };
    case!("guard-record", record);
    case!("guard-get-result", GuardGetResult { record: None });
    case!(
        "guard-get-result",
        GuardGetResult {
            record: Some(record)
        }
    );
    case!(
        "guard-rv-register",
        GuardRvRegister {
            topic: key,
            slot_id: slot,
            ttl_ms: 0,
            sealed_card: x
        }
    );
    case!(
        "guard-rv-slot",
        GuardRvSlot {
            topic: key,
            slot_id: slot
        }
    );
    case!(
        "guard-cards",
        GuardCards {
            cards: vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")]
        }
    );
    case!(
        "guard-dial",
        GuardDial {
            guard_key: key,
            route_token: [0; 32],
            udx_id: max,
            bucket: "b".into()
        }
    );
    case!(
        "guard-open",
        GuardOpen {
            route_token: key,
            udx_id: 0
        }
    );
    case!(
        "guard-ready",
        GuardReady {
            status: 2,
            udx_id: 0
        }
    );
    let dead = GuardCircuitDead { udx_id: 42 };
    case!("guard-circuit-dead", dead);
    for (kind, op, id, value) in [
        (0, 0, 7, compact_encoding::encode(&hello)),
        (1, 0, 7, compact_encoding::encode(&ack)),
        (2, 0, 7, compact_encoding::encode(&err)),
        (3, 11, 0, compact_encoding::encode(&dead)),
    ] {
        case!(
            "frame",
            Frame {
                kind,
                op,
                id,
                value
            }
        );
    }
    assert!(rows.next().is_none());
    let protocols: Vec<_> = [SEARCH, CAST, GUARD].iter().map(|p| {
        let ops: Vec<_> = p.ops.iter().map(|op| {
            let (mut row, shape) = if let Some(res) = op.response {
                (json!({"name":op.name,"kind":"request","request":op.value.name(),"response":res.name()}),res)
            } else { (json!({"name":op.name,"kind":"event","event":op.value.name()}),op.value) };
            if let Some(max) = shape.max_items() { row["maxItems"] = json!(max); row["flags"] = json!(shape.has_flags()); }
            row
        }).collect();
        json!({"protocol":p.name, "ops":ops})
    }).collect();
    assert_eq!(json!(protocols), fixture["protocols"]);
}

// A field appended in a later schema version reaches an older peer as trailing bytes: a value
// and a frame decode with one extra byte, to the same thing.
#[test]
fn trailing_bytes_after_a_value_or_a_frame_are_ignored() {
    use peartube_native::control_channel::decode_frame;
    use peartube_native::control_protocol::{Body, Struct};
    let open = GuardOpen {
        route_token: [9; 32],
        udx_id: 42,
    };
    let mut value = compact_encoding::encode(&open).to_vec();
    value.push(0x7f);
    assert_eq!(
        Struct::GuardOpen.decode(Bytes::from(value.clone())),
        Some(Body::GuardOpen(open))
    );
    let frame = Frame {
        kind: 0,
        op: 9,
        id: 3,
        value: Bytes::from(value),
    };
    let mut bytes = compact_encoding::encode(&frame).to_vec();
    bytes.push(0x7f);
    assert_eq!(decode_frame(Bytes::from(bytes)), Some(frame));
}
