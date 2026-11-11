# Identify and artwork port notes

## Identify

`identify.js` parses release names into title, year, episode, resolution, codec, audio and extension. It probes a link with HTTP HEAD (following redirects), using content type, filename query parameter, Content-Disposition and path to distinguish direct media from pages. The disposition's `filename=` may be quoted (`filename="film.mkv"`, which may hold spaces and semicolons) or a bare token (`filename=film.mkv`) that ends at `;` or whitespace and is never trimmed, exactly as `/filename=(?:"([^"]*)"|([^;\s]+))/i` reads it; a path whose percent escapes are malformed (`/dl/100%`) keeps its raw segment instead of failing the identification. For pages it runs `ubit info URL --json`, with an optional `UBIT_CONFIG_DIR`, a 45-second timeout, DRM detection and status-tagged errors. It derives page details and chooses the largest reported resolution and size. `identify` searches Cinemeta for files and release-like page titles, ranks related title/year candidates, auto-matches only strong matches, and creates a SHA-256-derived clip ID or an IMDb candidate ID.

`downloadPage` runs `ubit get URL --no-library --no-playlist --json -o DIR`, parsing NDJSON progress, process, done and error events. It supports cancellation, startup/stall/process timeouts and reports a path and optional safe integer size. It kills stalled or cancelled processes and maps missing executables and DRM/HTTP errors.

Cinemeta search performs HTTP GET `/catalog/{movie|series}/top/search={encoded-title}.json` with an eight-second timeout, validates IMDb IDs, filters and ranks results, and returns at most six. It suppresses search failures in `identify`.

## Artwork and HTTP

`art.js` has a buffered HTTP(S) client with a user agent, five redirects, configurable timeout and response byte limit; HEAD discards the body. Metadata lookup GETs `/meta/{movie|series}/{imdb}.json`, choosing the first response with a poster when available. It caches successes and negative results (five-minute negative TTL), clearing after 2,000 entries. Poster returns the poster URL; backdrop returns background or falls back to poster. The default Cinemeta endpoint is `https://v3-cinemeta.strem.io`. Artwork itself is not downloaded here. No subprocess is used in this module.

## Proposed Rust shape

Use typed serializable link, parsed-name, candidate, identification and progress records, preserving JS nulls, IDs and error/status behavior. Separate pure parsing and ranking from an async identification service with injected HTTP and process interfaces for deterministic tests. Implement HTTP with bounded streaming, redirect and timeout controls; share it with metadata lookup. Wrap `ubit` using Tokio process and asynchronous pipe reading, explicit cancellation and child termination, retaining its NDJSON and timeout semantics. Keep the metadata cache shared and synchronized, including negative expiry and its existing key behavior. Fixture-test pure behavior against JS and test HTTP/process boundaries with controlled local servers and executable substitutes before IPC integration.
