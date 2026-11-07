//! QR rendering for pairing links, ported from `src/qr.js`: an ISO/IEC 18004 encoder for versions
//! 1–14, byte mode, error-correction level M, with the same mask selection and SVG shape.

const V_M: [Option<Version>; 15] = [
    None,
    Some(Version::new(26, 10, 1, 16, 0, 0)),
    Some(Version::new(44, 16, 1, 28, 0, 0)),
    Some(Version::new(70, 26, 1, 44, 0, 0)),
    Some(Version::new(100, 18, 2, 32, 0, 0)),
    Some(Version::new(134, 24, 2, 43, 0, 0)),
    Some(Version::new(172, 16, 4, 27, 0, 0)),
    Some(Version::new(196, 18, 4, 31, 0, 0)),
    Some(Version::new(242, 22, 2, 38, 2, 39)),
    Some(Version::new(292, 22, 3, 36, 2, 37)),
    Some(Version::new(346, 26, 4, 43, 1, 44)),
    Some(Version::new(404, 30, 1, 50, 4, 51)),
    Some(Version::new(466, 22, 6, 36, 2, 37)),
    Some(Version::new(532, 22, 8, 37, 1, 38)),
    Some(Version::new(581, 24, 4, 40, 5, 41)),
];

const ALIGN: [&[usize]; 15] = [
    &[],
    &[],
    &[6, 18],
    &[6, 22],
    &[6, 26],
    &[6, 30],
    &[6, 34],
    &[6, 22, 38],
    &[6, 24, 42],
    &[6, 26, 46],
    &[6, 28, 50],
    &[6, 30, 54],
    &[6, 32, 58],
    &[6, 34, 62],
    &[6, 26, 46, 66],
];

/// `[totalCodewords, ecPerBlock, group1Blocks, group1DataCw, group2Blocks, group2DataCw]`.
#[derive(Clone, Copy)]
struct Version {
    ec_len: usize,
    g1_blocks: usize,
    g1_data: usize,
    g2_blocks: usize,
    g2_data: usize,
}

impl Version {
    const fn new(
        _total: usize,
        ec_len: usize,
        g1_blocks: usize,
        g1_data: usize,
        g2_blocks: usize,
        g2_data: usize,
    ) -> Self {
        Self {
            ec_len,
            g1_blocks,
            g1_data,
            g2_blocks,
            g2_data,
        }
    }

    fn data_codewords(&self) -> usize {
        self.g1_blocks * self.g1_data + self.g2_blocks * self.g2_data
    }
}

struct Gf {
    exp: [u8; 512],
    log: [u8; 256],
}

static GF: std::sync::LazyLock<Gf> = std::sync::LazyLock::new(|| {
    let mut exp = [0u8; 512];
    let mut log = [0u8; 256];
    let mut x: usize = 1;
    for (i, slot) in exp.iter_mut().enumerate().take(255) {
        *slot = x as u8;
        log[x] = i as u8;
        x <<= 1;
        if x & 0x100 != 0 {
            x ^= 0x11d;
        }
    }
    for i in 255..512 {
        exp[i] = exp[i - 255];
    }
    Gf { exp, log }
});

fn gf_mul(a: u8, b: u8) -> u8 {
    if a == 0 || b == 0 {
        0
    } else {
        GF.exp[usize::from(GF.log[usize::from(a)]) + usize::from(GF.log[usize::from(b)])]
    }
}

fn rs_generator(degree: usize) -> Vec<u8> {
    let mut poly = vec![1u8];
    for i in 0..degree {
        let mut next = vec![0u8; poly.len() + 1];
        let root = GF.exp[i];
        for (j, term) in poly.iter().enumerate() {
            next[j] ^= term;
            next[j + 1] ^= gf_mul(*term, root);
        }
        poly = next;
    }
    poly
}

fn rs_remainder(data: &[u8], degree: usize) -> Vec<u8> {
    let generator = rs_generator(degree);
    let mut rem = vec![0u8; degree];
    for byte in data {
        let factor = byte ^ rem[0];
        rem.copy_within(1.., 0);
        rem[degree - 1] = 0;
        for (j, e) in rem.iter_mut().enumerate() {
            *e ^= gf_mul(generator[j + 1], factor);
        }
    }
    rem
}

fn bch_format(ec_bits: u32, mask: u32) -> u32 {
    let data = (ec_bits << 3) | mask;
    let mut rem = data << 10;
    for i in (10..=14).rev() {
        if (rem >> i) & 1 == 1 {
            rem ^= 0x537 << (i - 10);
        }
    }
    ((data << 10) | rem) ^ 0x5412
}

fn bch_version(ver: u32) -> u32 {
    let mut rem = ver << 12;
    for i in (12..=17).rev() {
        if (rem >> i) & 1 == 1 {
            rem ^= 0x1f25 << (i - 12);
        }
    }
    (ver << 12) | rem
}

struct Codewords {
    ver: usize,
    bytes: Vec<u8>,
}

fn build_codewords(input: &[u8]) -> Codewords {
    let mut ver = 1;
    while ver < V_M.len() {
        let body = V_M[ver].expect("version table");
        let cap = body.data_codewords();
        let header_bits = 4 + if ver <= 9 { 8 } else { 16 };
        if header_bits + input.len() * 8 <= cap * 8 {
            break;
        }
        ver += 1;
    }
    assert!(ver < V_M.len(), "QR payload exceeds Version 14 capacity");
    let body = V_M[ver].expect("version table");
    let total_data = body.data_codewords();
    let mut bits: Vec<u8> = Vec::new();
    let push_bits = |val: u32, len: usize, bits: &mut Vec<u8>| {
        for i in (0..len).rev() {
            bits.push(((val >> i) & 1) as u8);
        }
    };
    push_bits(0b0100, 4, &mut bits);
    push_bits(input.len() as u32, if ver <= 9 { 8 } else { 16 }, &mut bits);
    for byte in input {
        push_bits(u32::from(*byte), 8, &mut bits);
    }
    let max_bits = total_data * 8;
    let terminator = 4usize.min(max_bits - bits.len());
    push_bits(0, terminator, &mut bits);
    while !bits.len().is_multiple_of(8) {
        bits.push(0);
    }
    let mut data = vec![0u8; total_data];
    for (i, chunk) in bits.chunks(8).enumerate() {
        let mut byte = 0u8;
        for bit in chunk {
            byte = (byte << 1) | *bit;
        }
        data[i] = byte;
    }
    let mut pad = 0;
    for slot in data.iter_mut().skip(bits.len() / 8) {
        *slot = if pad == 0 { 0xec } else { 0x11 };
        pad ^= 1;
    }

    let mut blocks: Vec<&[u8]> = Vec::new();
    let mut ecs: Vec<Vec<u8>> = Vec::new();
    let mut offset = 0;
    for _ in 0..body.g1_blocks {
        let block = &data[offset..offset + body.g1_data];
        offset += body.g1_data;
        blocks.push(block);
        ecs.push(rs_remainder(block, body.ec_len));
    }
    for _ in 0..body.g2_blocks {
        let block = &data[offset..offset + body.g2_data];
        offset += body.g2_data;
        blocks.push(block);
        ecs.push(rs_remainder(block, body.ec_len));
    }

    let mut out = Vec::with_capacity(total_data + body.ec_len * blocks.len());
    let max_block = body.g1_data.max(body.g2_data);
    for i in 0..max_block {
        for block in &blocks {
            if i < block.len() {
                out.push(block[i]);
            }
        }
    }
    for i in 0..body.ec_len {
        for ec in &ecs {
            out.push(ec[i]);
        }
    }
    Codewords { ver, bytes: out }
}

fn penalty_score(mat: &[u8], size: usize) -> u32 {
    let mut score = 0u32;
    for y in 0..size {
        let (mut run_h, mut run_v) = (1u32, 1u32);
        for x in 1..size {
            if mat[y * size + x] == mat[y * size + x - 1] {
                run_h += 1;
                if run_h == 5 {
                    score += 3;
                } else if run_h > 5 {
                    score += 1;
                }
            } else {
                run_h = 1;
            }
            if mat[x * size + y] == mat[(x - 1) * size + y] {
                run_v += 1;
                if run_v == 5 {
                    score += 3;
                } else if run_v > 5 {
                    score += 1;
                }
            } else {
                run_v = 1;
            }
        }
    }
    for y in 0..size - 1 {
        for x in 0..size - 1 {
            let c = mat[y * size + x];
            if c == mat[y * size + x + 1]
                && c == mat[(y + 1) * size + x]
                && c == mat[(y + 1) * size + x + 1]
            {
                score += 3;
            }
        }
    }
    let dark: u32 = mat.iter().filter(|m| **m != 0).count() as u32;
    let pct = f64::from(dark) * 100.0 / mat.len() as f64;
    let k = ((pct - 50.0).abs() / 5.0).floor() as u32;
    score + k * 10
}

fn mask_fn(index: usize, x: usize, y: usize) -> bool {
    let (x, y) = (x as u64, y as u64);
    match index {
        0 => (x + y) % 2 == 0,
        1 => y % 2 == 0,
        2 => x % 3 == 0,
        3 => (x + y) % 3 == 0,
        4 => (y / 2 + x / 3) % 2 == 0,
        5 => (x * y) % 2 + (x * y) % 3 == 0,
        6 => ((x * y) % 2 + (x * y) % 3) % 2 == 0,
        _ => ((x + y) % 2 + (x * y) % 3) % 2 == 0,
    }
}

struct Matrix {
    size: usize,
    modules: Vec<u8>,
}

fn encode_matrix(text: &str) -> Matrix {
    let codewords = build_codewords(text.as_bytes());
    let size = codewords.ver * 4 + 17;
    let mut base = vec![0u8; size * size];
    let mut is_func = vec![0u8; size * size];

    let set_fn = |base: &mut [u8], is_func: &mut [u8], x: isize, y: isize, dark: bool| {
        if x < 0 || y < 0 || x as usize >= size || y as usize >= size {
            return;
        }
        let idx = y as usize * size + x as usize;
        base[idx] = u8::from(dark);
        is_func[idx] = 1;
    };

    let finder = |base: &mut [u8], is_func: &mut [u8], ox: isize, oy: isize| {
        for dy in -1isize..=7 {
            for dx in -1isize..=7 {
                let in_outer = (0..=6).contains(&dx) && (0..=6).contains(&dy);
                let on_ring = dx == 0 || dx == 6 || dy == 0 || dy == 6;
                let in_core = (2..=4).contains(&dx) && (2..=4).contains(&dy);
                set_fn(
                    base,
                    is_func,
                    ox + dx,
                    oy + dy,
                    in_outer && (on_ring || in_core),
                );
            }
        }
    };
    finder(&mut base, &mut is_func, 0, 0);
    finder(&mut base, &mut is_func, size as isize - 7, 0);
    finder(&mut base, &mut is_func, 0, size as isize - 7);

    for i in 8..size - 8 {
        let dark = i % 2 == 0;
        set_fn(&mut base, &mut is_func, i as isize, 6, dark);
        set_fn(&mut base, &mut is_func, 6, i as isize, dark);
    }

    let positions = ALIGN[codewords.ver];
    for (i, cx) in positions.iter().enumerate() {
        for (j, cy) in positions.iter().enumerate() {
            if (i == 0 && (j == 0 || j == positions.len() - 1))
                || (i == positions.len() - 1 && j == 0)
            {
                continue;
            }
            for dy in -2isize..=2 {
                for dx in -2isize..=2 {
                    set_fn(
                        &mut base,
                        &mut is_func,
                        *cx as isize + dx,
                        *cy as isize + dy,
                        dx.abs().max(dy.abs()) != 1,
                    );
                }
            }
        }
    }

    for i in 0..=8 {
        if i != 6 {
            set_fn(&mut base, &mut is_func, 8, i, false);
            set_fn(&mut base, &mut is_func, i, 8, false);
        }
    }
    for i in 0..8 {
        set_fn(&mut base, &mut is_func, (size - 1 - i) as isize, 8, false);
        set_fn(&mut base, &mut is_func, 8, (size - 1 - i) as isize, false);
    }
    set_fn(&mut base, &mut is_func, 8, size as isize - 8, true);

    if codewords.ver >= 7 {
        let vbits = bch_version(codewords.ver as u32);
        for i in 0..18 {
            let bit = (vbits >> i) & 1 == 1;
            let a = size - 11 + (i % 3);
            let b = i / 3;
            set_fn(&mut base, &mut is_func, a as isize, b as isize, bit);
            set_fn(&mut base, &mut is_func, b as isize, a as isize, bit);
        }
    }

    let mut bit_idx = 0usize;
    let total_bits = codewords.bytes.len() * 8;
    let mut upward = true;
    let mut right = size as isize - 1;
    while right >= 1 {
        if right == 6 {
            right = 5;
        }
        for vert in 0..size {
            let y = if upward { size - 1 - vert } else { vert };
            for j in 0..2 {
                let x = (right - j) as usize;
                if is_func[y * size + x] != 0 {
                    continue;
                }
                let mut dark = false;
                if bit_idx < total_bits {
                    dark = (codewords.bytes[bit_idx >> 3] >> (7 - (bit_idx & 7))) & 1 == 1;
                    bit_idx += 1;
                }
                base[y * size + x] = u8::from(dark);
            }
        }
        upward = !upward;
        right -= 2;
    }

    let write_format = |mat: &mut [u8], mask_idx: u32| {
        let bits = bch_format(0b00, mask_idx); // ECC level M
        for i in 0..=5 {
            mat[i * size + 8] = ((bits >> i) & 1) as u8;
        }
        mat[7 * size + 8] = ((bits >> 6) & 1) as u8;
        mat[8 * size + 8] = ((bits >> 7) & 1) as u8;
        mat[8 * size + 7] = ((bits >> 8) & 1) as u8;
        for i in 9..15 {
            mat[8 * size + (14 - i)] = ((bits >> i) & 1) as u8;
        }
        for i in 0..8 {
            mat[8 * size + (size - 1 - i)] = ((bits >> i) & 1) as u8;
        }
        for i in 8..15 {
            mat[(size - 15 + i) * size + 8] = ((bits >> i) & 1) as u8;
        }
        mat[(size - 8) * size + 8] = 1;
    };

    let mut best: Option<Vec<u8>> = None;
    let mut best_score = u32::MAX;
    for m in 0..8 {
        let mut cand = base.clone();
        for y in 0..size {
            for x in 0..size {
                if is_func[y * size + x] == 0 && mask_fn(m, x, y) {
                    cand[y * size + x] ^= 1;
                }
            }
        }
        write_format(&mut cand, m as u32);
        let score = penalty_score(&cand, size);
        if score < best_score {
            best_score = score;
            best = Some(cand);
        }
    }

    Matrix {
        size,
        modules: best.unwrap_or(base),
    }
}

/// `qrSvg(text)`: the same 4-module margin and one-path-per-run SVG.
pub fn qr_svg(text: &str) -> String {
    let Matrix { size, modules } = encode_matrix(text);
    let margin = 4usize;
    let full = size + margin * 2;
    let mut paths = String::new();
    for y in 0..size {
        let mut x = 0;
        while x < size {
            if modules[y * size + x] == 0 {
                x += 1;
                continue;
            }
            let mut run = 1;
            while x + run < size && modules[y * size + x + run] != 0 {
                run += 1;
            }
            paths.push_str(&format!(
                "M{},{}h{}v1h-{}z",
                x + margin,
                y + margin,
                run,
                run
            ));
            x += run;
        }
    }
    format!(
        "<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 {full} {full}\" shape-rendering=\"crispEdges\"><rect width=\"{full}\" height=\"{full}\" fill=\"#ffffff\"/><path d=\"{paths}\" fill=\"#000000\"/></svg>"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_payload_uses_version_one() {
        let matrix = encode_matrix("hi");
        assert_eq!(matrix.size, 21);
    }

    #[test]
    fn long_payload_picks_a_bigger_version_and_still_renders() {
        let text = "peartube://receiver/".to_owned() + &"a".repeat(120);
        let matrix = encode_matrix(&text);
        assert!(matrix.size > 21);
        let svg = qr_svg(&text);
        assert!(svg.starts_with("<svg"));
        assert!(svg.ends_with("</svg>"));
    }
}
