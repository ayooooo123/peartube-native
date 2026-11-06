//! BIP-39 12/24-word vault and XSalsa20 epoch key wrapping, ported from the app's
//! `src/mnemonic.js`: a recovery phrase deterministically breeds one genesis writer, a detached
//! owner claim signer and a paid-epoch vault, without the devices sharing a writer Hypercore.

use std::fmt;
use std::sync::LazyLock;

use compact_encoding::hypercore::{HashAlgo, Manifest, SignatureAlgo, Signer};
use hypercore::crypto::{self, KeyPair};
use sha2::{Digest, Sha256};
use xsalsa20poly1305::aead::{Aead, KeyInit};
use xsalsa20poly1305::{Nonce, XSalsa20Poly1305};

use crate::protocol::{hex_decode, hex_encode, is_hex64, is_hex128, is_js_space, js_trim};

/// BIP-39 English words, copied from `src/mnemonic.js`.
static WORDS: LazyLock<Vec<&'static str>> = LazyLock::new(|| {
    let words: Vec<&'static str> = include_str!("wordlist.txt").lines().collect();
    assert_eq!(words.len(), 2048, "BIP-39 English list");
    words
});

/// The phrase was not a valid 12- or 24-word BIP-39 mnemonic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MnemonicError;

impl fmt::Display for MnemonicError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Invalid 12- or 24-word recovery phrase")
    }
}

impl std::error::Error for MnemonicError {}

/// A phrase-derived identity: a genesis writer, the channel key it signs, the detached owner
/// signer and the vault root epoch keys are derived from.
#[derive(Debug, Clone)]
pub struct Identity {
    pub mnemonic: String,
    pub genesis_key_pair: KeyPair,
    pub channel_key: String,
    pub owner_public_key: String,
    pub vault_key: [u8; 32],
    owner_key_pair: KeyPair,
}

impl Identity {
    /// `signClaim`.
    pub fn sign_claim(&self, target_channel_hex: &str, writer_hex: &str) -> String {
        let message = format!("peartube:claim:{target_channel_hex}:{writer_hex}");
        hex_encode(&self.owner_key_pair.sign(message.as_bytes()))
    }

    /// `epochKey`.
    pub fn epoch_key(&self, channel_hex: &str, epoch: u64) -> [u8; 32] {
        derive_epoch_key(&self.vault_key, channel_hex, epoch)
    }
}

/// `generateMnemonic`: 12 words unless `word_count == 24`.
pub fn generate_mnemonic(word_count: usize) -> String {
    let entropy: [u8; 32] = rand::random();
    if word_count == 24 {
        entropy_to_mnemonic(&entropy).expect("32-byte entropy")
    } else {
        entropy_to_mnemonic(&entropy[..16]).expect("16-byte entropy")
    }
}

/// `normalizeMnemonic`.
pub fn normalize_mnemonic(phrase: &str) -> String {
    js_trim(phrase)
        .to_lowercase()
        .split(is_js_space)
        .filter(|w| !w.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

/// `validateMnemonic`.
pub fn validate_mnemonic(phrase: &str) -> bool {
    let normalized = normalize_mnemonic(phrase);
    let words: Vec<&str> = normalized.split(' ').collect();
    if words.len() != 12 && words.len() != 24 {
        return false;
    }
    let mut bits = Vec::with_capacity(words.len() * 11);
    for word in &words {
        let Ok(index) = WORDS.binary_search(word) else {
            return false;
        };
        push_bits(&mut bits, index as u32, 11);
    }
    let entropy_bits = if words.len() == 12 { 128 } else { 256 };
    let checksum_bits = entropy_bits / 32;
    let entropy = bits_to_bytes(&bits[..entropy_bits]);
    let digest = Sha256::digest(entropy);
    (0..checksum_bits).all(|i| bits[entropy_bits + i] == (digest[0] >> (7 - i)) & 1)
}

/// `deriveIdentity`.
pub fn derive_identity(phrase: &str) -> Result<Identity, MnemonicError> {
    let mnemonic = normalize_mnemonic(phrase);
    if !validate_mnemonic(&mnemonic) {
        return Err(MnemonicError);
    }
    let seed = sha256(&[b"peartube:mnemonic:v1:", mnemonic.as_bytes()]);
    let genesis_key_pair = KeyPair::from_seed(&sha256(&[b"peartube:genesis:v1:", &seed]));
    let channel_key = hex_encode(&crypto::manifest_hash(&Manifest {
        version: 1,
        hash: HashAlgo::Blake2b,
        allow_patch: false,
        quorum: 1,
        signers: vec![Signer {
            signature: SignatureAlgo::Ed25519,
            namespace: crypto::default_namespace(),
            public_key: genesis_key_pair.public_key,
        }],
        prologue: None,
        linked: None,
        user_data: None,
    }));
    let owner_key_pair = KeyPair::from_seed(&sha256(&[b"peartube:owner:v1:", &seed]));
    let owner_public_key = hex_encode(&owner_key_pair.public_key);
    let vault_key = sha256(&[b"peartube:vault:v1:", &seed]);
    Ok(Identity {
        mnemonic,
        genesis_key_pair,
        channel_key,
        owner_public_key,
        vault_key,
        owner_key_pair,
    })
}

/// `verifyClaim`.
pub fn verify_claim(
    channel_hex: &str,
    writer_hex: &str,
    owner_pub_hex: &str,
    sig_hex: &str,
) -> bool {
    if !is_hex64(channel_hex)
        || !is_hex64(writer_hex)
        || !is_hex64(owner_pub_hex)
        || !is_hex128(sig_hex)
    {
        return false;
    }
    let Some(owner) = hex_decode(owner_pub_hex).and_then(|v| <[u8; 32]>::try_from(v).ok()) else {
        return false;
    };
    let Some(signature) = hex_decode(sig_hex).and_then(|v| <[u8; 64]>::try_from(v).ok()) else {
        return false;
    };
    crypto::verify(
        format!("peartube:claim:{channel_hex}:{writer_hex}").as_bytes(),
        &signature,
        &owner,
    )
}

fn derive_epoch_key(vault_key: &[u8; 32], channel_hex: &str, epoch: u64) -> [u8; 32] {
    sha256(&[
        b"peartube:epoch:v1:",
        vault_key,
        b":",
        channel_hex.to_lowercase().as_bytes(),
        b":",
        epoch.to_string().as_bytes(),
    ])
}

/// `wrapVideoKey`: `nonce || MAC || ciphertext`, hex encoded, with a random nonce.
pub fn wrap_video_key(video_key: &[u8], epoch_key: &[u8; 32]) -> String {
    let nonce: [u8; 24] = rand::random();
    let cipher = XSalsa20Poly1305::new_from_slice(epoch_key).expect("32-byte key");
    // This crate's `encrypt` is libsodium's secretbox layout: MAC || ciphertext.
    let sealed = cipher
        .encrypt(Nonce::from_slice(&nonce), video_key)
        .expect("secretbox encrypt");
    let mut out = Vec::with_capacity(24 + sealed.len());
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&sealed);
    hex_encode(&out)
}

/// `unwrapVideoKey`: the inverse of [`wrap_video_key`], or `None` if the box does not
/// authenticate.
pub fn unwrap_video_key(wrapped_hex: &str, epoch_key: &[u8; 32]) -> Option<Vec<u8>> {
    if !crate::protocol::is_hex144(wrapped_hex) {
        return None;
    }
    let raw = hex_decode(wrapped_hex)?;
    XSalsa20Poly1305::new_from_slice(epoch_key)
        .ok()?
        .decrypt(Nonce::from_slice(&raw[..24]), &raw[24..])
        .ok()
}

/// `formatAccessUri`.
pub fn format_access_uri(channel_hex: &str, epoch: u64, epoch_key_hex: &str) -> String {
    format!(
        "peartube://access/{}:{epoch}:{}",
        channel_hex.to_lowercase(),
        epoch_key_hex.to_lowercase()
    )
}

fn sha256(parts: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part);
    }
    hasher.finalize().into()
}

fn push_bits(out: &mut Vec<u8>, value: u32, width: u32) {
    for shift in (0..width).rev() {
        out.push(((value >> shift) & 1) as u8);
    }
}

fn bits_to_bytes(bits: &[u8]) -> Vec<u8> {
    bits.chunks(8)
        .map(|chunk| chunk.iter().fold(0u8, |byte, bit| (byte << 1) | bit))
        .collect()
}

fn entropy_to_mnemonic(entropy: &[u8]) -> Option<String> {
    if entropy.len() != 16 && entropy.len() != 32 {
        return None;
    }
    let checksum_bits = entropy.len() / 4;
    let digest = Sha256::digest(entropy);
    let mut bits = Vec::with_capacity(entropy.len() * 8 + checksum_bits);
    for &byte in entropy {
        push_bits(&mut bits, u32::from(byte), 8);
    }
    push_bits(&mut bits, u32::from(digest[0]), 8);
    bits.truncate(entropy.len() * 8 + checksum_bits);
    let words: Vec<&str> = bits
        .chunks(11)
        .map(|chunk| {
            WORDS[chunk
                .iter()
                .fold(0usize, |n, bit| (n << 1) | usize::from(*bit))]
        })
        .collect();
    Some(words.join(" "))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wordlist_is_sorted_bip39() {
        assert_eq!(WORDS.len(), 2048);
        assert!(WORDS.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(WORDS[0], "abandon");
        assert_eq!(WORDS[2047], "zoo");
    }

    #[test]
    fn all_zero_vectors() {
        let phrase = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";
        assert!(validate_mnemonic(phrase));
        assert_eq!(
            normalize_mnemonic(&format!("  {}\n", phrase.to_uppercase())),
            phrase
        );

        let phrase24 = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon art";
        assert!(validate_mnemonic(phrase24));
    }

    #[test]
    fn rejects_bad_checksum_and_counts() {
        assert!(!validate_mnemonic(""));
        assert!(!validate_mnemonic("abandon abandon abandon"));
        let bad = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon";
        assert!(!validate_mnemonic(bad));
        assert!(!validate_mnemonic(
            "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon notaword"
        ));
    }

    #[test]
    fn generated_phrases_validate() {
        for count in [12, 24, 0, 13] {
            let phrase = generate_mnemonic(count);
            assert!(validate_mnemonic(&phrase), "{phrase}");
            assert_eq!(phrase.split(' ').count(), if count == 24 { 24 } else { 12 });
        }
        assert_eq!(generate_mnemonic(24).split(' ').count(), 24);
    }

    #[test]
    fn wrap_roundtrip() {
        let epoch = sha256(&[b"epoch"]);
        let key = sha256(&[b"video"]);
        let wrapped = wrap_video_key(&key, &epoch);
        assert_eq!(wrapped.len(), 144);
        assert_eq!(
            unwrap_video_key(&wrapped, &epoch).as_deref(),
            Some(key.as_slice())
        );
        let other = sha256(&[b"other"]);
        assert_eq!(unwrap_video_key(&wrapped, &other), None);
        assert_eq!(unwrap_video_key(&wrapped[..142], &epoch), None);
    }
}
