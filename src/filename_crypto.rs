use hctr2_rs::Hctr2_128;

use crate::error::{Error, Result};

const MIN_PADDED_LEN: usize = 16;
const FILESYSTEM_NAME_LIMIT: usize = 255;

const ALPHABET: [u8; 91] =
    *b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!#$%&()*+,.':;<=>?@[]^_`{|}~\"";

const INVERSE: [u8; 256] = {
    let mut table = [0xFFu8; 256];
    let mut i = 0usize;
    while i < 91 {
        table[ALPHABET[i] as usize] = i as u8;
        i += 1;
    }
    table
};

fn base91_encode(src: &[u8]) -> String {
    let mut out = Vec::with_capacity(src.len() * 2);
    let mut acc: u32 = 0;
    let mut num_bits: u32 = 0;

    for &x in src {
        acc |= (x as u32) << num_bits;
        num_bits += 8;
        if num_bits > 13 {
            let mut v = acc & 0x1fff;
            if v > 88 {
                acc >>= 13;
                num_bits -= 13;
            } else {
                v = acc & 0x3fff;
                acc >>= 14;
                num_bits -= 14;
            }
            out.push(ALPHABET[(v % 91) as usize]);
            out.push(ALPHABET[(v / 91) as usize]);
        }
    }
    if num_bits > 0 {
        out.push(ALPHABET[(acc % 91) as usize]);
        if num_bits > 7 || acc > 90 {
            out.push(ALPHABET[(acc / 91) as usize]);
        }
    }
    // SAFETY: all bytes come from the ASCII alphabet
    unsafe { String::from_utf8_unchecked(out) }
}

fn base91_decode(src: &str) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(src.len());
    let mut acc: Option<u32> = None;
    let mut b: u32 = 0;
    let mut num_bits: u32 = 0;

    for &x in src.as_bytes() {
        let c = INVERSE[x as usize];
        if c == 0xFF {
            return Err(Error::Encryption(format!("invalid base91 character: {:?}", x as char)));
        }
        let c = c as u32;
        if let Some(a) = acc {
            let v = a + c * 91;
            b |= v << num_bits;
            num_bits += if (v & 0x1fff) > 88 { 13 } else { 14 };
            while num_bits > 7 {
                out.push(b as u8);
                b >>= 8;
                num_bits -= 8;
            }
            acc = None;
        } else {
            acc = Some(c);
        }
    }
    if let Some(a) = acc {
        let last = b | (a << num_bits);
        if last > 0xff {
            return Err(Error::Encryption("invalid base91 padding".into()));
        }
        out.push(last as u8);
    } else if b != 0 {
        return Err(Error::Encryption("invalid base91 padding".into()));
    }
    Ok(out)
}

pub fn encrypt_filename(name: &str, key: &[u8; 16]) -> Result<String> {
    if name == "." || name == ".." {
        return Ok(name.to_string());
    }

    let padded_len = name.len().max(MIN_PADDED_LEN);
    let mut padded = vec![0u8; padded_len];
    padded[..name.len()].copy_from_slice(name.as_bytes());

    let cipher = Hctr2_128::new(key);
    let mut ciphertext = vec![0u8; padded_len];
    cipher
        .encrypt(&padded, &[], &mut ciphertext)
        .map_err(|e| Error::Encryption(format!("HCTR2 encrypt: {e}")))?;

    let encoded = base91_encode(&ciphertext);
    if encoded.len() > FILESYSTEM_NAME_LIMIT {
        return Err(Error::Encryption(format!(
            "encrypted filename exceeds {FILESYSTEM_NAME_LIMIT} bytes ({} bytes)",
            encoded.len()
        )));
    }
    Ok(encoded)
}

pub fn decrypt_filename(name: &str, key: &[u8; 16]) -> Result<String> {
    if name == "." || name == ".." {
        return Ok(name.to_string());
    }

    let ciphertext = base91_decode(name)?;
    if ciphertext.len() < MIN_PADDED_LEN {
        return Err(Error::Encryption("ciphertext too short for HCTR2".into()));
    }

    let cipher = Hctr2_128::new(key);
    let mut padded = vec![0u8; ciphertext.len()];
    cipher
        .decrypt(&ciphertext, &[], &mut padded)
        .map_err(|e| Error::Encryption(format!("HCTR2 decrypt: {e}")))?;

    let actual_len = padded.iter().rposition(|&b| b != 0).map_or(0, |i| i + 1);
    String::from_utf8(padded[..actual_len].to_vec())
        .map_err(|e| Error::Encryption(format!("decrypted filename is not UTF-8: {e}")))
}

pub fn encrypt_path(path: &str, key: &[u8; 16]) -> Result<String> {
    if path.is_empty() {
        return Ok(String::new());
    }
    path.split('/')
        .filter(|c| !c.is_empty())
        .map(|c| encrypt_filename(c, key))
        .collect::<Result<Vec<_>>>()
        .map(|parts| parts.join("/"))
}

pub fn decrypt_path(path: &str, key: &[u8; 16]) -> Result<String> {
    if path.is_empty() {
        return Ok(String::new());
    }
    path.split('/')
        .filter(|c| !c.is_empty())
        .map(|c| decrypt_filename(c, key))
        .collect::<Result<Vec<_>>>()
        .map(|parts| parts.join("/"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_KEY: [u8; 16] = [0x42u8; 16];

    #[test]
    fn roundtrip_filenames() {
        for name in [
            "myfile.txt",
            "a",
            "ab",
            "hi.z",
            "0123456789abcdef",
            "this_is_a_very_long_filename_that_exceeds_the_minimum_padding_length_of_16_bytes.txt",
        ] {
            let enc = encrypt_filename(name, &TEST_KEY).unwrap();
            assert_ne!(enc, name);
            let dec = decrypt_filename(&enc, &TEST_KEY).unwrap();
            assert_eq!(dec, name, "failed roundtrip for {name:?}");
            // Deterministic
            assert_eq!(enc, encrypt_filename(name, &TEST_KEY).unwrap());
        }
        // Special entries pass through
        for dot in [".", ".."] {
            assert_eq!(encrypt_filename(dot, &TEST_KEY).unwrap(), dot);
            assert_eq!(decrypt_filename(dot, &TEST_KEY).unwrap(), dot);
        }
    }

    #[test]
    fn wrong_key_fails() {
        let enc = encrypt_filename("secret.txt", &TEST_KEY).unwrap();
        let wrong_key = [0x00u8; 16];
        let dec = decrypt_filename(&enc, &wrong_key);
        // Decryption itself succeeds (HCTR2 is length-preserving, no auth tag on the name),
        // but the result will be garbage — not the original name.
        match dec {
            Ok(name) => assert_ne!(name, "secret.txt"),
            Err(_) => {} // also acceptable (e.g. invalid UTF-8)
        }
    }

    #[test]
    fn path_roundtrip() {
        assert_eq!(encrypt_path("", &TEST_KEY).unwrap(), "");
        assert_eq!(decrypt_path("", &TEST_KEY).unwrap(), "");
        let enc = encrypt_path("a/b/c", &TEST_KEY).unwrap();
        assert!(!enc.contains("a/"));
        assert_eq!(decrypt_path(&enc, &TEST_KEY).unwrap(), "a/b/c");
        // Leading/trailing slashes are normalized
        let enc = encrypt_path("/a/b/", &TEST_KEY).unwrap();
        assert_eq!(decrypt_path(&enc, &TEST_KEY).unwrap(), "a/b");
    }

    #[test]
    fn encrypted_name_limits_and_output() {
        for len in [50, 100, 150, 200, 205] {
            let name: String = std::iter::repeat_n('a', len).collect();
            let enc = encrypt_filename(&name, &TEST_KEY).unwrap();
            assert!(
                enc.len() <= FILESYSTEM_NAME_LIMIT,
                "len {len} produced {} bytes",
                enc.len()
            );
        }
        assert!(encrypt_filename(&"a".repeat(220), &TEST_KEY).is_err());
        for name in ["file.txt", "model.safetensors", "data.parquet"] {
            let enc = encrypt_filename(name, &TEST_KEY).unwrap();
            assert!(!enc.contains('/'), "encrypted {name:?} contains slash: {enc}");
        }
    }

    #[test]
    fn base91_roundtrip() {
        assert_eq!(base91_encode(b""), "");
        assert_eq!(base91_decode("").unwrap(), b"");
        let data = b"hello world, this is a test of base91 encoding!";
        assert_eq!(base91_decode(&base91_encode(data)).unwrap(), data);
    }
}
