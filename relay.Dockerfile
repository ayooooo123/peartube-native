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
VOLUME /data
EXPOSE 8174 8175 8443
ENTRYPOINT ["/usr/local/bin/peartube-relay"]
