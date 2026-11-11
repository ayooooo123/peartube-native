//! SHA-256 whose running state is exportable in libsodium's layout, so a checkpointed ingest resumes after a
//! restart instead of re-reading the bytes already on disk.
//!
//! `crypto_hash_sha256_state` is what sodium-native hands the app (`libsodium`'s struct memcpy'd whole):
//! `uint32_t state[8]`, `uint64_t count`, `unsigned char buf[64]` — 104 bytes, no padding, and the integers in
//! host order, which every target this crate builds for stores little-endian. `count` is the **bit** length fed
//! so far (`update` adds `inlen << 3`) and `buf` holds the last `(count >> 3) % 64` bytes; the rest of `buf` is
//! never read.
//!
//! `tests/js/sha256-state.mjs` pins the layout against sodium-native: a state it exports is resumed here to the
//! same digest, and a state exported here is finalised there to the same digest.

/// `crypto_hash_sha256_STATEBYTES`.
pub const SHA256_STATE_BYTES: usize = 104;
/// `crypto_hash_sha256_BYTES`.
pub const SHA256_DIGEST_BYTES: usize = 32;

const BLOCK: usize = 64;

const IV: [u32; 8] = [
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
];

const K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

fn compress(h: &mut [u32; 8], block: &[u8]) {
    let mut w = [0u32; 64];
    for (i, word) in w.iter_mut().take(16).enumerate() {
        let at = i * 4;
        *word = u32::from_be_bytes([block[at], block[at + 1], block[at + 2], block[at + 3]]);
    }
    for i in 16..64 {
        let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
        let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
        w[i] = w[i - 16]
            .wrapping_add(s0)
            .wrapping_add(w[i - 7])
            .wrapping_add(s1);
    }
    let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = *h;
    for i in 0..64 {
        let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
        let ch = (e & f) ^ (!e & g);
        let t1 = hh
            .wrapping_add(s1)
            .wrapping_add(ch)
            .wrapping_add(K[i])
            .wrapping_add(w[i]);
        let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
        let maj = (a & b) ^ (a & c) ^ (b & c);
        let t2 = s0.wrapping_add(maj);
        hh = g;
        g = f;
        f = e;
        e = d.wrapping_add(t1);
        d = c;
        c = b;
        b = a;
        a = t1.wrapping_add(t2);
    }
    for (slot, value) in h.iter_mut().zip([a, b, c, d, e, f, g, hh]) {
        *slot = slot.wrapping_add(value);
    }
}

/// A running SHA-256 that can be snapshotted and restored.
#[derive(Clone)]
pub struct Sha256State {
    h: [u32; 8],
    /// The bit length fed so far, as libsodium keeps it.
    count: u64,
    buf: [u8; BLOCK],
}

impl Default for Sha256State {
    fn default() -> Self {
        Self::new()
    }
}

impl Sha256State {
    /// `crypto_hash_sha256_init`.
    pub fn new() -> Self {
        Self {
            h: IV,
            count: 0,
            buf: [0u8; BLOCK],
        }
    }

    /// The total bytes folded in so far (`count / 8`).
    pub fn bytes(&self) -> u64 {
        self.count / 8
    }

    fn buffered(&self) -> usize {
        ((self.count >> 3) % BLOCK as u64) as usize
    }

    /// `crypto_hash_sha256_update`.
    pub fn update(&mut self, mut data: &[u8]) {
        if data.is_empty() {
            return;
        }
        let fed = self.count;
        self.count += (data.len() as u64) << 3;
        let left = ((fed >> 3) % BLOCK as u64) as usize;
        if left > 0 {
            let fill = BLOCK - left;
            if data.len() < fill {
                self.buf[left..left + data.len()].copy_from_slice(data);
                return;
            }
            self.buf[left..].copy_from_slice(&data[..fill]);
            let block = self.buf;
            compress(&mut self.h, &block);
            data = &data[fill..];
        }
        while data.len() >= BLOCK {
            compress(&mut self.h, &data[..BLOCK]);
            data = &data[BLOCK..];
        }
        self.buf[..data.len()].copy_from_slice(data);
    }

    /// The libsodium state bytes a job carries: the words, the bit count, then the partial block (its unwritten
    /// tail zeroed, which libsodium never reads).
    pub fn to_bytes(&self) -> [u8; SHA256_STATE_BYTES] {
        let mut out = [0u8; SHA256_STATE_BYTES];
        for (i, word) in self.h.iter().enumerate() {
            out[i * 4..i * 4 + 4].copy_from_slice(&word.to_le_bytes());
        }
        out[32..40].copy_from_slice(&self.count.to_le_bytes());
        let left = self.buffered();
        out[40..40 + left].copy_from_slice(&self.buf[..left]);
        out
    }

    /// A state carried by a job, or nothing when it is not one libsodium wrote. Only the bytes `update` and
    /// `final` read are restored; the tail of `buf` is ignored, as libsodium ignores it.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != SHA256_STATE_BYTES {
            return None;
        }
        let mut state = Self::new();
        for i in 0..8 {
            state.h[i] = u32::from_le_bytes(bytes[i * 4..i * 4 + 4].try_into().ok()?);
        }
        state.count = u64::from_le_bytes(bytes[32..40].try_into().ok()?);
        let left = state.buffered();
        state.buf[..left].copy_from_slice(&bytes[40..40 + left]);
        Some(state)
    }

    /// `crypto_hash_sha256_final`.
    pub fn finalize(self) -> [u8; SHA256_DIGEST_BYTES] {
        let mut h = self.h;
        let mut block = [0u8; BLOCK];
        let left = self.buffered();
        block[..left].copy_from_slice(&self.buf[..left]);
        block[left] = 0x80;
        if left >= BLOCK - 8 {
            compress(&mut h, &block);
            block = [0u8; BLOCK];
        }
        block[BLOCK - 8..].copy_from_slice(&self.count.to_be_bytes());
        compress(&mut h, &block);
        let mut out = [0u8; SHA256_DIGEST_BYTES];
        for (i, word) in h.iter().enumerate() {
            out[i * 4..i * 4 + 4].copy_from_slice(&word.to_be_bytes());
        }
        out
    }
}

/// Standard-alphabet base64 as `b4a.toString(state, 'base64')` writes it (padded).
pub fn base64_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let second = chunk.get(1).copied().unwrap_or(0);
        let third = chunk.get(2).copied().unwrap_or(0);
        let n = (u32::from(chunk[0]) << 16) | (u32::from(second) << 8) | u32::from(third);
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(ALPHABET[((n >> (18 - i * 6)) & 0x3f) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// `b4a.from(text, 'base64')`: the standard alphabet, padding optional, `None` when a character is not base64.
pub fn base64_decode(text: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(text.len() / 4 * 3);
    let mut acc = 0u32;
    let mut bits = 0u32;
    let mut padding = false;
    for byte in text.bytes() {
        if byte == b'=' {
            padding = true;
            continue;
        }
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            b'\r' | b'\n' => continue,
            _ => return None,
        };
        if padding {
            return None;
        }
        acc = (acc << 6) | u32::from(value);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    fn digest_of(data: &[u8]) -> String {
        Sha256::digest(data)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }

    fn hex(digest: [u8; 32]) -> String {
        digest.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[test]
    fn digests_match_sha2() {
        for size in [0usize, 1, 3, 55, 56, 63, 64, 65, 127, 128, 1000, 512 * 1024 + 7] {
            let data: Vec<u8> = (0..size).map(|i| (i * 31 + 7) as u8).collect();
            let mut state = Sha256State::new();
            // fed in awkward pieces, as a network stream arrives
            for piece in data.chunks(37) {
                state.update(piece);
            }
            assert_eq!(hex(state.finalize()), digest_of(&data), "{size} bytes");
        }
    }

    #[test]
    fn base64_round_trips() {
        for bytes in [vec![], vec![0u8], vec![1, 2], vec![1, 2, 3], vec![7u8; 104]] {
            assert_eq!(
                base64_decode(&base64_encode(&bytes)).as_deref(),
                Some(bytes.as_slice())
            );
        }
        assert_eq!(base64_decode("not base64!"), None);
        assert_eq!(base64_decode("QQ==").as_deref(), Some(&b"A"[..]));
    }

    #[test]
    fn a_state_resumes_where_it_stopped() {
        let data: Vec<u8> = (0..200_000u32).map(|i| (i * 31 + 7) as u8).collect();
        for at in [0usize, 1, 63, 64, 65, 65_536, 199_999] {
            let mut head = Sha256State::new();
            head.update(&data[..at]);
            let bytes = head.to_bytes();
            assert_eq!(bytes.len(), SHA256_STATE_BYTES);
            let mut resumed = Sha256State::from_bytes(&bytes).expect("a libsodium state");
            assert_eq!(resumed.bytes(), at as u64);
            resumed.update(&data[at..]);
            assert_eq!(hex(resumed.finalize()), digest_of(&data), "resume at {at}");
        }
    }
}
