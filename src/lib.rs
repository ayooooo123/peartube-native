#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

pub mod mnemonic;
pub mod protocol;

pub use mnemonic::{Identity, MnemonicError, derive_identity};
pub use protocol::{ParsedUri, UriError};
