#!/bin/sh
# Builds one pinned engine artifact for an `ffi-*` release:
#
#   sh ffi/build.sh <target> <out-dir>
#
#   target    android-arm64 | darwin-arm64
#   out-dir   receives the asset; nothing is written outside it and target/
#
# The release workflow runs this unchanged on ubuntu-latest and macos-latest
# (.github/workflows/release-ffi.yml); ffi/README.md lists the assets.
set -eu

target=${1:?usage: build.sh <android-arm64|darwin-arm64> <out-dir>}
mkdir -p "${2:?usage: build.sh <android-arm64|darwin-arm64> <out-dir>}"
out=$(cd "$2" && pwd)
repo=$(cd "$(dirname "$0")/.." && pwd)

# rustc separates CARGO_ENCODED_RUSTFLAGS arguments with 0x1f.
us=$(printf '\037')
cargo_home=${CARGO_HOME:-$HOME/.cargo}
# No build-machine paths in panic messages or debug info: the libraries are published.
remap="--remap-path-prefix=$HOME=~"
remap="$remap${us}--remap-path-prefix=$cargo_home/registry=~/.cargo/registry"
remap="$remap${us}--remap-path-prefix=$cargo_home/git=~/.cargo/git"

# Builds for one hyphenated triple need that triple's dashes as underscores in
# CC_/CXX_/AR_ and upper-cased in CARGO_TARGET_<triple>_LINKER.
env_name() { printf '%s' "$1" | tr '-' '_'; }
var_name() { printf '%s' "$1" | tr 'a-z-' 'A-Z_'; }

case "$target" in
  android-arm64)
    triple=aarch64-linux-android
    asset=peartube_native_ffi-android-arm64.so

    ndk=${ANDROID_NDK_HOME:-}
    if [ -z "$ndk" ]; then
      sdk=${ANDROID_HOME:-${ANDROID_SDK_ROOT:-$HOME/Library/Android/sdk}}
      ndk=$(ls -d "$sdk"/ndk/* 2>/dev/null | sort -V | tail -n 1)
    fi
    if [ -z "$ndk" ] || [ ! -d "$ndk/toolchains/llvm/prebuilt" ]; then
      echo "no Android NDK: set ANDROID_NDK_HOME (nothing under \$ANDROID_HOME/ndk)" >&2
      exit 1
    fi
    # The NDK ships one host toolchain: darwin-x86_64 (Rosetta on arm Macs) or linux-x86_64.
    case $(uname -s) in
      Darwin) host=darwin-x86_64 ;;
      *) host=linux-x86_64 ;;
    esac
    bin=$ndk/toolchains/llvm/prebuilt/$host/bin
    cc=aarch64-linux-android29-clang
    [ -x "$bin/$cc" ] || { echo "no $cc under $ndk" >&2; exit 1; }

    name=$(env_name "$triple")
    export "CARGO_TARGET_$(var_name "$triple")_LINKER=$bin/$cc"
    export "CC_$name=$bin/$cc" "CXX_$name=$bin/$cc++" "AR_$name=$bin/llvm-ar"
    # bindgen (rocksdb) parses headers with its own clang: without the NDK sysroot it reads the host's /usr/include.
    export "BINDGEN_EXTRA_CLANG_ARGS_$name=--sysroot=$ndk/toolchains/llvm/prebuilt/$host/sysroot --target=aarch64-linux-android29"
    # Android 15 devices use 16 KiB pages; Rust's Android std links libc++_shared.so
    # dynamically, so that NDK copy must ship beside this file (see ffi/README.md).
    rustflags="-Clink-arg=-Wl,-z,max-page-size=16384"
    strip=$bin/llvm-strip
    built=$repo/target/$triple/release/libpeartube_native_ffi.so
    ;;
  darwin-arm64)
    triple=aarch64-apple-darwin
    asset=peartube_native_ffi-darwin-arm64.dylib
    # The host loads it from the app bundle as lib<name>.dylib, under @rpath.
    rustflags="-Clink-arg=-Wl,-install_name,@rpath/libpeartube_native_ffi.dylib"
    built=$repo/target/$triple/release/libpeartube_native_ffi.dylib
    ;;
  *)
    echo "unsupported target $target (android-arm64 | darwin-arm64)" >&2
    exit 1
    ;;
esac

export CARGO_ENCODED_RUSTFLAGS="$rustflags${us}$remap"
cargo build --manifest-path "$repo/Cargo.toml" -p peartube-native-ffi --release --target "$triple"

# A released asset is frozen: fail the build rather than publish a wrong one.
case "$target" in
  android-arm64)
    "$strip" --strip-unneeded -o "$out/$asset" "$built"
    "$bin/llvm-readelf" -l "$out/$asset" | grep -Eq '^  LOAD .*0x4000$' ||
      { echo "$asset: LOAD segments are not 16 KiB aligned" >&2; exit 1; }
    [ "$("$bin/llvm-nm" -D --defined-only "$out/$asset" | grep -c ' T pt_')" -eq 6 ] ||
      { echo "$asset: the six pt_ symbols are not all exported" >&2; exit 1; }
    ;;
  darwin-arm64)
    cp "$built" "$out/$asset"
    [ "$(otool -D "$out/$asset" | sed -n 2p)" = "@rpath/libpeartube_native_ffi.dylib" ] ||
      { echo "$asset: install name is not @rpath/libpeartube_native_ffi.dylib" >&2; exit 1; }
    [ "$(nm -gU "$out/$asset" | grep -c ' _pt_')" -eq 6 ] ||
      { echo "$asset: the six pt_ symbols are not all exported" >&2; exit 1; }
    ;;
esac

# Home paths must not reach a published artifact.
if strings "$out/$asset" | grep -Eq '/(Users|home)/'; then
  echo "$asset: contains a build-machine path" >&2
  exit 1
fi

echo "$out/$asset: $(wc -c < "$out/$asset" | tr -d ' ') bytes, $triple"
