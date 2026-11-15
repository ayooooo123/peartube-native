# The Rust relay image (`peartube-relay`), a drop-in for the JS relay image: same env, ports
# and /data store. Build context: this stack's crates under `stack/` (peartube-native and its
# path dependencies) and the PearTube app's `src/ui.js` and `web/assets` under `peartube/`.
#
#   podman build -f stack/peartube-native/relay.Dockerfile -t peartube-relay-rs <context>

# The relay UI is the app's `src/ui.js` page, rendered once to index.html.
FROM docker.io/library/node:22-bookworm-slim AS web
WORKDIR /app
COPY peartube/src/ui.js src/ui.js
COPY peartube/web/assets web/assets
RUN node -e "import('./src/ui.js').then(m => require('fs').writeFileSync('web/index.html', m.page))" \
  && mkdir /web && cp web/index.html web/assets/peartube_web.wasm web/assets/peartube_web.wasm.gz web/assets/player-worker.js /web/

FROM docker.io/library/rust:1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends clang libclang-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY stack ./
WORKDIR /src/peartube-native
RUN --mount=type=cache,target=/usr/local/cargo/registry --mount=type=cache,target=/src/peartube-native/target \
  cargo build --release --locked --bin peartube-relay && cp target/release/peartube-relay /peartube-relay

# RocksDB needs glibc, libstdc++ and libgcc_s, which distroless/cc carries with CA certificates.
FROM gcr.io/distroless/cc-debian12
COPY --from=build /peartube-relay /usr/local/bin/peartube-relay
COPY --from=web /web /usr/local/bin/web
ENV PEARTUBE_STORAGE=/data
# glibc raises its mmap threshold to the largest block freed so far (up to 32 MiB) and its trim
# threshold to twice that, and the relay frees a 16 MiB block within minutes of starting: each of
# its 17 arenas could then keep 32 MiB free at its top (650 MB resident, about 200 MiB in use).
# A fixed trim threshold of 8 MiB bounds that. The mmap threshold stays high on purpose: at glibc's
# 128 KiB default every block buffer was mapped and unmapped per use (127,000 page faults a second,
# 30 CPU-s per GiB served against 11-17 with the arena).
ENV GLIBC_TUNABLES=glibc.malloc.mmap_threshold=33554432:glibc.malloc.trim_threshold=8388608
VOLUME /data
EXPOSE 8174 8175 8443
ENTRYPOINT ["/usr/local/bin/peartube-relay"]
