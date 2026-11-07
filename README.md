# peartube-native

Native backend for PearTube: the channel protocol and mnemonic identity, wire-identical to the
app's `src/protocol.js` and `src/mnemonic.js`.

## What it does

- `protocol`: entry/blob constants, `is_uniform_512`, `describe_entry`, `clean_text`, `clean_name`,
  watch/receiver/access URI formatting, `parse_uri`, `decode_op`, `is_system_view_key`.
- `mnemonic`: BIP-39 generation/validation, `derive_identity` (genesis writer, channel key,
  detached owner signer, vault root), claim signing/verification, epoch keys, and XSalsa20-Poly1305
  video key wrapping.
- `art`: the relay's small bounded http(s) client (redirects, timeout, byte cap), the Cinemeta
  metadata lookup with its negative cache, and poster/backdrop URLs.
- `identify`: `parse_name`, the HEAD probe that tells a media file from a page, `ubit info`/`ubit get`
  with the app's argv, statuses and NDJSON progress, Cinemeta suggestions, and the `clip:` id.

A Rust node derives the same channel keys, owner keys, claim signatures, epoch keys and wrapped
video keys as the JS app from the same recovery phrase, and parses/formats the same URIs and ops.

## JavaScript parity

The app is the reference. `fixtures/js-reference.json` is generated from the app's own logic
(copied into `fixtures/gen`) and checked by `tests/js_fixtures.rs`:

```sh
cd fixtures/gen && npm install && npm run gen
cargo test -- --include-ignored
```

`--include-ignored` also un-ignores the cross-check that opens Rust-produced wrappings with the JS
reference; it reports a skip when `fixtures/gen/node_modules` is absent.

`fixtures/art-identify-reference.json` is generated the same way from the app's `src/art.js` and
`src/identify.js`, and replayed by `tests/art_identify.rs` (node only, no npm install needed):

```sh
cd fixtures/gen && node art-identify.mjs && cd ../.. && cargo test --test art_identify
```

Parity notes:

- Lengths and slices use UTF-16 code units, and truncation applies `toWellFormed` semantics, so a
  cut surrogate pair becomes U+FFFD exactly like the app.
- JS whitespace, `encodeURIComponent`/`decodeURIComponent`, and `Number(string)` semantics are
  reproduced for the values these formats carry.
- Rust strings cannot hold lone surrogates, so a JS string that already contains one cannot be
  represented through this API; JSON itself cannot portably carry it either.
- Receiver ports are `f64` and are serialized as integers when integral; exponent-form port numbers
  (`1e+21`) keep Rust's formatting rather than JavaScript's.
- `art::send`'s timeout covers the whole exchange; the app's `req.setTimeout` only covered socket
  inactivity, so a slow trickle keeps waiting there.
- A reply over `max_bytes` is a normal error here; the app's `request.destroy(err)` surfaces as an
  uncaught socket error instead, so that case is only asserted on the Rust side.

## License

MIT OR Apache-2.0.
