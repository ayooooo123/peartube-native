use peartube_native::node::api::*;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

fn roundtrip<T: DeserializeOwned + Serialize>(value: Value) {
    let decoded: T = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), value);
}

#[test]
fn node_json_shapes() {
    let route = json!({"peers":0,"rxBytes":0,"txBytes":0,"downBps":0,"upBps":0});
    let source =
        json!({"key":"channel-key","kind":"channel","alias":"","primary":true,"writable":true});
    let blob = json!({"blockOffset":0,"blockLength":1,"byteOffset":0,"byteLength":3});
    let announce = json!({"type":"announce","id":"tt1234567","title":"Film","size":3,"sha256":"hash","blobs":"core","blob":blob});
    let entry = json!({"key":"entry","id":"tt1234567","title":"Film","size":3,"sha256":"hash","local":true,"cachedBytes":3,"private":false,"epoch":0,"locked":false,"source":"channel-key","sourceKind":"channel","season":null,"episode":null,"seriesKey":"film","baseTitle":"Film","year":null,"streamUrl":"http://127.0.0.1/","magnetUrl":"pear://watch"});
    roundtrip::<Route>(route.clone());
    let routes = json!({"private":route,"public":route,"lan":route});
    roundtrip::<Routes>(routes.clone());
    let reciprocity =
        json!({"tier":"leecher","seededBytes":0,"contributingPeers":0,"ratio":0,"inflightMax":0});
    roundtrip::<Reciprocity>(reciprocity.clone());
    roundtrip::<Status>(
        json!({"tracker":"","channel":"","writer":"","writable":false,"blobs":"","blobBytes":0,"cachedBytes":0,"cacheMaxBytes":0,"peers":0,"lanPeers":0,"downBps":0,"upBps":0,"rxBytes":0,"txBytes":0,"routes":routes,"inflightBlocks":0,"rtt":0,"retransmits":0,"sources":[],"reciprocity":reciprocity}),
    );
    roundtrip::<Source>(source.clone());
    roundtrip::<Source>(
        json!({"key":"key","kind":"channel","alias":"","primary":false,"writable":false,"entries":3,"active":true,"hydrated":true}),
    );
    roundtrip::<FollowedSource>(json!({"key":"key","kind":"channel","alias":""}));
    roundtrip::<DiscoveredSource>(
        json!({"key":"key","kind":"channel","title":"Film","via":"key2"}),
    );
    roundtrip::<PublishedSource>(json!({"key":"key","kind":"channel","title":"Film"}));
    roundtrip::<Blob>(blob);
    roundtrip::<Announce>(announce.clone());
    // From test/network.test.js:87.
    let zero = "0".repeat(64);
    roundtrip::<Announce>(
        json!({"type":"announce","id":"imdb:tt9999991","title":"Spam","size":1,"sha256":zero,"blobs":zero,"blob":{"blockOffset":0,"blockLength":1,"byteOffset":0,"byteLength":1}}),
    );
    roundtrip::<Entry>(entry.clone());
    roundtrip::<SearchOptions>(json!({"source":null,"query":null,"limit":10,"offset":0}));
    roundtrip::<SearchOptions>(json!({"source":"key","query":"film","limit":10.0,"offset":0.0}));
    roundtrip::<SourceSpec>(json!("pear://channel/key"));
    roundtrip::<SourceSpec>(json!({"key":"key","kind":"channel","alias":"home"}));
    roundtrip::<SourceObject>(json!({"uri":"pear://channel/key"}));
    roundtrip::<Removed>(json!({"removed":true}));
    roundtrip::<OkResult>(json!({"ok":true}));
    roundtrip::<SeedableCore>(json!({"key":"key","kind":"blobs","length":1,"byteLength":3}));
    roundtrip::<LanEndpoint>(json!({"host":"127.0.0.1","port":1234,"publicKey":"key"}));
    let writer = json!({"key":"key","role":"owner","alias":"primary","self":true});
    roundtrip::<Writer>(writer.clone());
    roundtrip::<Identity>(
        json!({"channel":"key","writer":"key","role":null,"writable":false,"mnemonicSet":false,"writers":[writer],"epoch":1}),
    );
    roundtrip::<Identity>(
        json!({"channel":"key","writer":"key","role":"owner","writable":true,"mnemonicSet":true,"mnemonic":"","writers":[],"epoch":1,"epochKey":"key","accessUri":"pear://access"}),
    );
    roundtrip::<Grant>(json!({"granted":true,"writer":"key","role":"writer","alias":""}));
    roundtrip::<Revoke>(json!({"revoked":true,"writer":"key","purgeEntries":false}));
    roundtrip::<Claim>(json!({"claimed":false,"reopenNeeded":true,"channel":"key"}));
    roundtrip::<Claim>(
        json!({"claimed":true,"reopenNeeded":false,"channel":"key","writer":"key2"}),
    );
    roundtrip::<Epoch>(json!({"epoch":2,"epochKey":"key","accessUri":"pear://access"}));
    roundtrip::<CacheCleared>(json!({"clearedBytes":1,"reclaimedBytes":2}));
    roundtrip::<CacheCeiling>(json!({"evicted":1,"cachedBytes":2}));
    roundtrip::<StorageGc>(json!({"beforeBytes":3,"afterBytes":2,"reclaimedBytes":1}));
    roundtrip::<SyncResult>(json!({"backfilled":1,"total":2}));
    roundtrip::<RechunkResult>(
        json!({"total":2,"migrated":1,"deduped":0,"remaining":1,"migratedBytes":3}),
    );
    roundtrip::<PutMeta>(
        json!({"id":"tt1234567","title":"Film","private":false,"expectedSize":3,"hashState":null,"hashBytes":0}),
    );
    roundtrip::<PutMeta>(json!({"id":"imdb:tt0068646","title":"Kept","expectedSize":null}));
    let mut published = announce.as_object().unwrap().clone();
    published.extend(json!({"key":"entry","channel":"key","streamUrl":"http://127.0.0.1/","magnetUrl":"pear://watch"}).as_object().unwrap().clone());
    roundtrip::<Published>(Value::Object(published));
    roundtrip::<OpenUri>(json!({"action":"source","source":source}));
    roundtrip::<OpenUri>(json!({"action":"access","channel":"key","epoch":1,"source":source}));
    roundtrip::<OpenUri>(json!({"action":"watch","entry":entry}));
    roundtrip::<CastStatus>(
        json!({"webReceiverUrl":null,"receivers":[],"session":null,"receiverMode":null}),
    );
    roundtrip::<ReceiverCommand>(json!({"seq":1,"action":"pause"}));
    for event in [
        json!({"event":"update"}),
        json!({"event":"sources","sources":[{"key":"key","kind":"channel","alias":""}]}),
        json!({"event":"cast","cast":{"webReceiverUrl":null,"receivers":[],"session":null,"receiverMode":null}}),
        json!({"event":"receiverCommand","command":{"seq":1,"action":"pause"}}),
        json!({"event":"fatal","error":"failure"}),
    ] {
        roundtrip::<WorkerEvent>(event);
    }
}
