use std::fmt;
use std::fs;
use std::io::{Read, Write};
use std::path::Path;

use aegis::raf::{Aegis128L, Aegis128X2, Aegis128X4, Aegis256, Aegis256X2, Aegis256X4, FileIo, Raf};

use crate::error::{Error, Result};

const DEFAULT_CHUNK_SIZE: u32 = 65536;
const COPY_BUF_SIZE: usize = 65536;
const CONTENT_TYPE_PREFIX: &str = "application/vnd.hf-mount+enc";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Algorithm {
    Aegis128L,
    Aegis128X2,
    Aegis128X4,
    Aegis256,
    Aegis256X2,
    Aegis256X4,
}

macro_rules! define_algorithms {
    ($($variant:ident => $display:literal, $scheme:literal, $key_len:expr;)+) => {
        impl Algorithm {
            pub fn key_len(self) -> usize { match self { $(Self::$variant => $key_len,)+ } }
            fn scheme(self) -> &'static str { match self { $(Self::$variant => $scheme,)+ } }
            fn from_scheme(s: &str) -> Option<Self> { match s { $($scheme => Some(Self::$variant),)+ _ => None } }
        }
        impl fmt::Display for Algorithm {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(match self { $(Self::$variant => $display,)+ })
            }
        }
        impl std::str::FromStr for Algorithm {
            type Err = Error;
            fn from_str(s: &str) -> Result<Self> {
                match s { $($display => Ok(Self::$variant),)+ _ => Err(Error::Encryption(format!("unknown algorithm: {s}"))) }
            }
        }
    };
}

define_algorithms! {
    Aegis128L  => "aegis-128l",  "aegis-128l-raf",  16;
    Aegis128X2 => "aegis-128x2", "aegis-128x2-raf", 16;
    Aegis128X4 => "aegis-128x4", "aegis-128x4-raf", 16;
    Aegis256   => "aegis-256",   "aegis-256-raf",   32;
    Aegis256X2 => "aegis-256x2", "aegis-256x2-raf", 32;
    Aegis256X4 => "aegis-256x4", "aegis-256x4-raf", 32;
}

#[derive(Clone)]
pub struct EncryptionConfig {
    pub prk: [u8; 32],
    pub source_context: String,
    pub algorithm: Algorithm,
    pub chunk_size: u32,
    pub filename_key: [u8; 16],
}

const HKDF_SALT: &[u8] = b"hf-mount-v1";
const INFO_PREFIX: &[u8] = b"hf-mount-derive-v1";
const FILENAME_INFO_PREFIX: &[u8] = b"hf-mount-filename-v1";
pub const MASTER_KEY_LEN: usize = 32;

impl EncryptionConfig {
    pub fn derive_key(&self, algorithm: Algorithm) -> Vec<u8> {
        derive_key(&self.prk, &self.source_context, algorithm)
    }
}

pub fn extract_prk(master_key: &[u8; MASTER_KEY_LEN]) -> [u8; 32] {
    hmac_sha256::HKDF::extract(HKDF_SALT, master_key)
}

fn build_info(source_context: &str, algorithm: Algorithm) -> Vec<u8> {
    let scheme = algorithm.scheme();
    let mut info = Vec::with_capacity(INFO_PREFIX.len() + 2 + source_context.len() + 2 + scheme.len());
    info.extend_from_slice(INFO_PREFIX);
    info.extend_from_slice(&(source_context.len() as u16).to_be_bytes());
    info.extend_from_slice(source_context.as_bytes());
    info.extend_from_slice(&(scheme.len() as u16).to_be_bytes());
    info.extend_from_slice(scheme.as_bytes());
    info
}

pub fn derive_key(prk: &[u8; 32], source_context: &str, algorithm: Algorithm) -> Vec<u8> {
    let info = build_info(source_context, algorithm);
    let mut derived = vec![0u8; algorithm.key_len()];
    hmac_sha256::HKDF::expand(&mut derived, prk, &info);
    derived
}

pub fn derive_filename_key(prk: &[u8; 32], source_context: &str) -> [u8; 16] {
    let mut info = Vec::with_capacity(FILENAME_INFO_PREFIX.len() + 2 + source_context.len());
    info.extend_from_slice(FILENAME_INFO_PREFIX);
    info.extend_from_slice(&(source_context.len() as u16).to_be_bytes());
    info.extend_from_slice(source_context.as_bytes());
    let mut key = [0u8; 16];
    hmac_sha256::HKDF::expand(&mut key, prk, &info);
    key
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncryptedFileInfo {
    pub algorithm: Algorithm,
    pub plaintext_size: u64,
    pub ciphertext_size: u64,
    pub chunk_size: u32,
}

pub fn load_master_key(path: &Path) -> Result<[u8; MASTER_KEY_LEN]> {
    let raw = fs::read(path).map_err(|e| Error::Encryption(format!("cannot read key file {}: {e}", path.display())))?;

    if raw.len() == MASTER_KEY_LEN {
        return Ok(raw.try_into().unwrap());
    }

    let trimmed = std::str::from_utf8(&raw).ok().map(|s| s.trim()).unwrap_or("");
    if !trimmed.is_empty()
        && let Some(decoded) = base64_decode(trimmed)
        && decoded.len() == MASTER_KEY_LEN
    {
        return Ok(decoded.try_into().unwrap());
    }

    Err(Error::Encryption(format!(
        "key file must contain exactly {MASTER_KEY_LEN} bytes (raw) or base64-encoded, got {} bytes",
        raw.len()
    )))
}

fn base64_decode(s: &str) -> Option<Vec<u8>> {
    use base64::{
        Engine, alphabet,
        engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig},
    };
    const LENIENT: GeneralPurposeConfig =
        GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent);
    const STANDARD: GeneralPurpose = GeneralPurpose::new(&alphabet::STANDARD, LENIENT);
    const URL_SAFE: GeneralPurpose = GeneralPurpose::new(&alphabet::URL_SAFE, LENIENT);
    STANDARD.decode(s).ok().or_else(|| URL_SAFE.decode(s).ok())
}

pub fn format_content_type(info: &EncryptedFileInfo) -> String {
    format!(
        "{}; scheme={}; chunk={}; size={}; stored={}",
        CONTENT_TYPE_PREFIX,
        info.algorithm.scheme(),
        info.chunk_size,
        info.plaintext_size,
        info.ciphertext_size,
    )
}

pub fn parse_content_type(ct: &str) -> Result<Option<EncryptedFileInfo>> {
    if !ct.starts_with(CONTENT_TYPE_PREFIX) {
        return Ok(None);
    }

    let params = &ct[CONTENT_TYPE_PREFIX.len()..];
    let mut scheme: Option<Algorithm> = None;
    let mut chunk: Option<u32> = None;
    let mut size: Option<u64> = None;
    let mut stored: Option<u64> = None;

    for param in params.split(';') {
        let param = param.trim();
        if param.is_empty() {
            continue;
        }
        if let Some((key, val)) = param.split_once('=') {
            match key.trim() {
                "scheme" => {
                    scheme = Some(
                        Algorithm::from_scheme(val.trim())
                            .ok_or_else(|| Error::Encryption(format!("unknown encryption scheme: {}", val.trim())))?,
                    );
                }
                "chunk" => {
                    chunk = Some(
                        val.trim()
                            .parse()
                            .map_err(|_| Error::Encryption(format!("invalid chunk size: {}", val.trim())))?,
                    );
                }
                "size" => {
                    size = Some(
                        val.trim()
                            .parse()
                            .map_err(|_| Error::Encryption(format!("invalid plaintext size: {}", val.trim())))?,
                    );
                }
                "stored" => {
                    stored = Some(
                        val.trim()
                            .parse()
                            .map_err(|_| Error::Encryption(format!("invalid ciphertext size: {}", val.trim())))?,
                    );
                }
                _ => {}
            }
        }
    }

    let algorithm = scheme.ok_or_else(|| Error::Encryption("missing scheme in encryption metadata".into()))?;
    let plaintext_size = size.ok_or_else(|| Error::Encryption("missing size in encryption metadata".into()))?;
    let ciphertext_size =
        stored.ok_or_else(|| Error::Encryption("missing stored size in encryption metadata".into()))?;
    let chunk_size = chunk.unwrap_or(DEFAULT_CHUNK_SIZE);

    Ok(Some(EncryptedFileInfo {
        algorithm,
        plaintext_size,
        ciphertext_size,
        chunk_size,
    }))
}

pub enum RafHandle {
    Aegis128L(Raf<Aegis128L>),
    Aegis128X2(Raf<Aegis128X2>),
    Aegis128X4(Raf<Aegis128X4>),
    Aegis256(Raf<Aegis256>),
    Aegis256X2(Raf<Aegis256X2>),
    Aegis256X4(Raf<Aegis256X4>),
}

macro_rules! dispatch {
    ($self:expr, $method:ident $(, $arg:expr)*) => {
        match $self {
            RafHandle::Aegis128L(r) => r.$method($($arg),*),
            RafHandle::Aegis128X2(r) => r.$method($($arg),*),
            RafHandle::Aegis128X4(r) => r.$method($($arg),*),
            RafHandle::Aegis256(r) => r.$method($($arg),*),
            RafHandle::Aegis256X2(r) => r.$method($($arg),*),
            RafHandle::Aegis256X4(r) => r.$method($($arg),*),
        }
    };
}

impl RafHandle {
    pub fn read(&mut self, buf: &mut [u8], offset: u64) -> Result<usize> {
        dispatch!(self, read, buf, offset).map_err(raf_err)
    }

    pub fn write(&mut self, data: &[u8], offset: u64) -> Result<usize> {
        dispatch!(self, write, data, offset).map_err(raf_err)
    }

    pub fn truncate(&mut self, size: u64) -> Result<()> {
        dispatch!(self, truncate, size).map_err(raf_err)
    }

    pub fn size(&self) -> u64 {
        dispatch!(self, size)
    }

    pub fn sync(&mut self) -> Result<()> {
        dispatch!(self, sync).map_err(raf_err)
    }
}

fn raf_err(e: aegis::raf::Error) -> Error {
    Error::Encryption(format!("RAF error: {e}"))
}

pub fn create_raf(path: &Path, key: &[u8], algorithm: Algorithm, chunk_size: u32) -> Result<RafHandle> {
    macro_rules! create {
        ($algo:ty, $key_len:literal) => {{
            let key: [u8; $key_len] = key
                .try_into()
                .map_err(|_| Error::Encryption(format!("key length mismatch for {algorithm}")))?;
            let io = FileIo::create(path)?;
            let raf = aegis::raf::RafBuilder::<$algo>::new()
                .chunk_size(chunk_size)
                .truncate(true)
                .create(io, &key)
                .map_err(raf_err)?;
            Ok(raf.into())
        }};
    }
    match algorithm {
        Algorithm::Aegis128L => create!(Aegis128L, 16),
        Algorithm::Aegis128X2 => create!(Aegis128X2, 16),
        Algorithm::Aegis128X4 => create!(Aegis128X4, 16),
        Algorithm::Aegis256 => create!(Aegis256, 32),
        Algorithm::Aegis256X2 => create!(Aegis256X2, 32),
        Algorithm::Aegis256X4 => create!(Aegis256X4, 32),
    }
}

pub fn open_raf(path: &Path, key: &[u8], algorithm: Algorithm) -> Result<RafHandle> {
    macro_rules! open {
        ($algo:ty, $key_len:literal) => {{
            let key: [u8; $key_len] = key
                .try_into()
                .map_err(|_| Error::Encryption(format!("key length mismatch for {algorithm}")))?;
            let raf = Raf::<$algo>::open_file(path, &key).map_err(raf_err)?;
            Ok(raf.into())
        }};
    }
    match algorithm {
        Algorithm::Aegis128L => open!(Aegis128L, 16),
        Algorithm::Aegis128X2 => open!(Aegis128X2, 16),
        Algorithm::Aegis128X4 => open!(Aegis128X4, 16),
        Algorithm::Aegis256 => open!(Aegis256, 32),
        Algorithm::Aegis256X2 => open!(Aegis256X2, 32),
        Algorithm::Aegis256X4 => open!(Aegis256X4, 32),
    }
}

macro_rules! impl_from_raf {
    ($($variant:ident),+) => {
        $(impl From<Raf<$variant>> for RafHandle {
            fn from(r: Raf<$variant>) -> Self { Self::$variant(r) }
        })+
    };
}
impl_from_raf!(Aegis128L, Aegis128X2, Aegis128X4, Aegis256, Aegis256X2, Aegis256X4);

pub fn encrypt_file(src: &Path, dst: &Path, key: &[u8], algorithm: Algorithm, chunk_size: u32) -> Result<()> {
    let mut input = fs::File::open(src)?;
    let mut raf = create_raf(dst, key, algorithm, chunk_size)?;
    let mut buf = vec![0u8; COPY_BUF_SIZE];
    let mut offset: u64 = 0;
    loop {
        let n = input.read(&mut buf)?;
        if n == 0 {
            break;
        }
        raf.write(&buf[..n], offset)?;
        offset += n as u64;
    }
    raf.sync()?;
    Ok(())
}

pub fn decrypt_file(src: &Path, dst: &Path, key: &[u8], info: &EncryptedFileInfo) -> Result<u64> {
    let mut raf = open_raf(src, key, info.algorithm)?;
    let plaintext_size = raf.size();

    if plaintext_size != info.plaintext_size {
        return Err(Error::Encryption(format!(
            "RAF plaintext size ({}) disagrees with metadata ({})",
            plaintext_size, info.plaintext_size
        )));
    }
    let ciphertext_len = fs::metadata(src)?.len();
    if ciphertext_len != info.ciphertext_size {
        return Err(Error::Encryption(format!(
            "ciphertext file size ({}) disagrees with metadata ({})",
            ciphertext_len, info.ciphertext_size
        )));
    }
    let mut output = fs::File::create(dst)?;
    let mut buf = vec![0u8; COPY_BUF_SIZE];
    let mut offset: u64 = 0;
    while offset < plaintext_size {
        let to_read = ((plaintext_size - offset) as usize).min(COPY_BUF_SIZE);
        let n = raf.read(&mut buf[..to_read], offset)?;
        if n == 0 {
            break;
        }
        output.write_all(&buf[..n])?;
        offset += n as u64;
    }
    output.flush()?;
    Ok(plaintext_size)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn tmp_dir() -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("hf-mount-crypto-{}-{}", std::process::id(), id));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    const TEST_MASTER_KEY: [u8; MASTER_KEY_LEN] = [0x42u8; MASTER_KEY_LEN];
    const TEST_SOURCE: &str = "bucket/test";

    fn test_key(algorithm: Algorithm) -> Vec<u8> {
        let prk = extract_prk(&TEST_MASTER_KEY);
        derive_key(&prk, TEST_SOURCE, algorithm)
    }

    #[test]
    fn parse_non_encrypted() {
        assert!(parse_content_type("application/octet-stream").unwrap().is_none());
        assert!(parse_content_type("text/plain").unwrap().is_none());
    }

    #[test]
    fn format_and_parse_roundtrip() {
        let info = EncryptedFileInfo {
            algorithm: Algorithm::Aegis128X2,
            plaintext_size: 1048576,
            ciphertext_size: 1049600,
            chunk_size: 65536,
        };
        let ct = format_content_type(&info);
        let parsed = parse_content_type(&ct).unwrap().unwrap();
        assert_eq!(parsed, info);
    }

    #[test]
    fn format_and_parse_all_algorithms() {
        for alg in [
            Algorithm::Aegis128L,
            Algorithm::Aegis128X2,
            Algorithm::Aegis128X4,
            Algorithm::Aegis256,
            Algorithm::Aegis256X2,
            Algorithm::Aegis256X4,
        ] {
            let info = EncryptedFileInfo {
                algorithm: alg,
                plaintext_size: 100,
                ciphertext_size: 200,
                chunk_size: 4096,
            };
            let ct = format_content_type(&info);
            let parsed = parse_content_type(&ct).unwrap().unwrap();
            assert_eq!(parsed.algorithm, alg);
        }
    }

    #[test]
    fn parse_malformed() {
        for ct in [
            "application/vnd.hf-mount+enc; chunk=65536; size=100; stored=200", // missing scheme
            "application/vnd.hf-mount+enc; scheme=aegis-128x2-raf; chunk=65536; stored=200", // missing size
            "application/vnd.hf-mount+enc; scheme=aegis-128x2-raf; chunk=65536; size=100", // missing stored
            "application/vnd.hf-mount+enc; scheme=chacha20-raf; chunk=65536; size=100; stored=200", // unknown scheme
            "application/vnd.hf-mount+enc; scheme=aegis-128x2-raf; chunk=65536; size=abc; stored=200", // bad number
        ] {
            assert!(parse_content_type(ct).is_err(), "should reject: {ct}");
        }
    }

    #[test]
    fn algorithm_display_and_parse() {
        for alg in [
            Algorithm::Aegis128L,
            Algorithm::Aegis128X2,
            Algorithm::Aegis128X4,
            Algorithm::Aegis256,
            Algorithm::Aegis256X2,
            Algorithm::Aegis256X4,
        ] {
            let s = alg.to_string();
            let parsed: Algorithm = s.parse().unwrap();
            assert_eq!(parsed, alg);
        }
    }

    #[test]
    fn algorithm_parse_unknown() {
        assert!("aes-gcm".parse::<Algorithm>().is_err());
    }

    #[test]
    fn key_loading_raw() {
        let dir = tmp_dir();
        let key_path = dir.join("key.bin");
        fs::write(&key_path, &[0xABu8; 32]).unwrap();
        let key = load_master_key(&key_path).unwrap();
        assert_eq!(key, [0xABu8; 32]);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn key_loading_base64() {
        let dir = tmp_dir();
        let key_path = dir.join("key.b64");
        // 32 bytes of 0xAB base64-encoded
        let b64 = "q6urq6urq6urq6urq6urq6urq6urq6urq6urq6urq6s=";
        fs::write(&key_path, b64).unwrap();
        let key = load_master_key(&key_path).unwrap();
        assert_eq!(key, [0xABu8; 32]);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn key_loading_wrong_size() {
        let dir = tmp_dir();
        let key_path = dir.join("key.bin");
        fs::write(&key_path, &[0xABu8; 16]).unwrap();
        assert!(load_master_key(&key_path).is_err());
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn raf_create_write_read_roundtrip() {
        let dir = tmp_dir();
        let raf_path = dir.join("test.raf");
        let alg = Algorithm::Aegis128X2;
        let key = test_key(alg);

        let mut raf = create_raf(&raf_path, &key, alg, DEFAULT_CHUNK_SIZE).unwrap();
        raf.write(b"hello world", 0).unwrap();
        raf.sync().unwrap();
        assert_eq!(raf.size(), 11);

        let mut buf = vec![0u8; 11];
        raf.read(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"hello world");

        drop(raf);

        let mut raf = open_raf(&raf_path, &key, alg).unwrap();
        let mut buf = vec![0u8; 11];
        raf.read(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"hello world");

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn raf_random_write_read() {
        let dir = tmp_dir();
        let raf_path = dir.join("random.raf");
        let alg = Algorithm::Aegis128X2;
        let key = test_key(alg);

        let mut raf = create_raf(&raf_path, &key, alg, DEFAULT_CHUNK_SIZE).unwrap();
        raf.write(b"AAAA", 0).unwrap();
        raf.write(b"BB", 2).unwrap();

        let mut buf = vec![0u8; 4];
        raf.read(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"AABB");

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn raf_truncate() {
        let dir = tmp_dir();
        let raf_path = dir.join("trunc.raf");
        let alg = Algorithm::Aegis128X2;
        let key = test_key(alg);

        let mut raf = create_raf(&raf_path, &key, alg, DEFAULT_CHUNK_SIZE).unwrap();
        raf.write(&vec![0xBBu8; 4096], 0).unwrap();
        assert_eq!(raf.size(), 4096);

        raf.truncate(1024).unwrap();
        assert_eq!(raf.size(), 1024);

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn raf_wrong_key() {
        let dir = tmp_dir();
        let raf_path = dir.join("wrongkey.raf");
        let alg = Algorithm::Aegis128X2;
        let key = test_key(alg);

        let mut raf = create_raf(&raf_path, &key, alg, DEFAULT_CHUNK_SIZE).unwrap();
        raf.write(b"secret data", 0).unwrap();
        raf.sync().unwrap();
        drop(raf);

        let wrong_key = vec![0x00u8; 16];
        let open_result = open_raf(&raf_path, &wrong_key, alg);
        match open_result {
            Err(_) => {}
            Ok(mut raf) => {
                let mut buf = vec![0u8; 11];
                assert!(raf.read(&mut buf, 0).is_err());
            }
        }

        fs::remove_dir_all(&dir).unwrap();
    }

    fn roundtrip_test(algorithm: Algorithm, data: &[u8]) {
        let dir = tmp_dir();
        let key = test_key(algorithm);
        let pt_path = dir.join("plain.bin");
        let ct_path = dir.join("cipher.raf");
        let dec_path = dir.join("decrypted.bin");
        fs::write(&pt_path, data).unwrap();
        encrypt_file(&pt_path, &ct_path, &key, algorithm, DEFAULT_CHUNK_SIZE).unwrap();
        let ct_size = fs::metadata(&ct_path).unwrap().len();
        assert!(ct_size > data.len() as u64);
        let info = EncryptedFileInfo {
            algorithm,
            plaintext_size: data.len() as u64,
            ciphertext_size: ct_size,
            chunk_size: DEFAULT_CHUNK_SIZE,
        };
        let pt_size = decrypt_file(&ct_path, &dec_path, &key, &info).unwrap();
        assert_eq!(pt_size, data.len() as u64);
        assert_eq!(fs::read(&dec_path).unwrap(), data);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn encrypt_decrypt_file_roundtrip() {
        roundtrip_test(Algorithm::Aegis128X2, b"the quick brown fox jumps over the lazy dog");
        roundtrip_test(Algorithm::Aegis256, b"256-bit key test data");
        let large: Vec<u8> = (0..200_000).map(|i| (i % 251) as u8).collect();
        roundtrip_test(Algorithm::Aegis128X2, &large);
    }

    #[test]
    fn content_type_default_chunk_size() {
        let ct = "application/vnd.hf-mount+enc; scheme=aegis-128x2-raf; size=100; stored=200";
        let info = parse_content_type(ct).unwrap().unwrap();
        assert_eq!(info.chunk_size, DEFAULT_CHUNK_SIZE);
    }

    #[test]
    fn hkdf_properties() {
        let prk = extract_prk(&TEST_MASTER_KEY);
        // Deterministic
        let k1 = derive_key(&prk, TEST_SOURCE, Algorithm::Aegis128X2);
        assert_eq!(k1, derive_key(&prk, TEST_SOURCE, Algorithm::Aegis128X2));
        // Source separation
        assert_ne!(
            derive_key(&prk, "bucket/alice/data", Algorithm::Aegis128X2),
            derive_key(&prk, "bucket/bob/data", Algorithm::Aegis128X2)
        );
        // Namespace separation
        assert_ne!(
            derive_key(&prk, "bucket/user/foo", Algorithm::Aegis128X2),
            derive_key(&prk, "model/user/foo", Algorithm::Aegis128X2)
        );
        // Algorithm separation
        assert_ne!(k1, derive_key(&prk, TEST_SOURCE, Algorithm::Aegis256));
        // Correct output length per algorithm
        for alg in [
            Algorithm::Aegis128L,
            Algorithm::Aegis128X2,
            Algorithm::Aegis128X4,
            Algorithm::Aegis256,
            Algorithm::Aegis256X2,
            Algorithm::Aegis256X4,
        ] {
            assert_eq!(derive_key(&prk, TEST_SOURCE, alg).len(), alg.key_len());
        }
        // Non-identity (derived key differs from master key)
        assert_ne!(
            &derive_key(&prk, TEST_SOURCE, Algorithm::Aegis256)[..],
            &TEST_MASTER_KEY[..]
        );
    }
}
