#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

pub mod cast;
pub mod control_channel;
pub mod control_protocol;
pub mod lan;
pub mod mnemonic;
pub mod protocol;

pub use mnemonic::{Identity, MnemonicError, derive_identity};
pub use protocol::{ParsedUri, UriError};
pub mod acquire;
pub mod adapters;
pub mod app;
pub mod app_settings;
pub mod art;
pub mod backend;
pub mod hiverelay;
pub mod http;
pub mod identify;
pub mod identity;
pub mod ipc;
pub mod lifecycle;
pub mod media_check;
pub mod mirror;
pub mod node;
pub mod private_overlay;
pub mod providers;
pub mod relay;
pub mod replicate;
pub mod resolution;
pub mod search;
pub mod settings;
pub mod ssrf;
mod suspend;
pub mod tags;
pub mod traffic;
pub mod wire {
    include!(concat!(env!("OUT_DIR"), "/peartube_wire.rs"));
}
