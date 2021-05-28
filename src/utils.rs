use std::convert::TryInto;

use anyhow::Result;
use sha2::Digest;
use std::ops::{Index, IndexMut, Range, RangeFrom};
use ton_api::{BoxedSerialize, IntoBoxed, Serializer};

pub struct PacketView<'a> {
    bytes: &'a mut [u8],
}

impl<'a> PacketView<'a> {
    pub const fn get(&self) -> &[u8] {
        self.bytes
    }

    pub fn get_mut(&mut self) -> &mut [u8] {
        self.bytes
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub fn remove_prefix(&mut self, prefix_len: usize) {
        let len = self.bytes.len();
        let ptr = self.bytes.as_mut_ptr();
        // SAFETY: `bytes` is already a reference bounded by a lifetime
        self.bytes = unsafe { std::slice::from_raw_parts_mut(ptr.add(len), len - prefix_len) };
    }
}

impl Index<Range<usize>> for PacketView<'_> {
    type Output = [u8];

    fn index(&self, index: Range<usize>) -> &Self::Output {
        self.bytes.index(index)
    }
}

impl IndexMut<Range<usize>> for PacketView<'_> {
    fn index_mut(&mut self, index: Range<usize>) -> &mut Self::Output {
        self.bytes.index_mut(index)
    }
}

impl Index<RangeFrom<usize>> for PacketView<'_> {
    type Output = [u8];

    fn index(&self, index: RangeFrom<usize>) -> &Self::Output {
        self.bytes.index(index)
    }
}

impl IndexMut<RangeFrom<usize>> for PacketView<'_> {
    fn index_mut(&mut self, index: RangeFrom<usize>) -> &mut Self::Output {
        self.bytes.index_mut(index)
    }
}

impl<'a> From<&'a mut [u8]> for PacketView<'a> {
    fn from(bytes: &'a mut [u8]) -> Self {
        Self { bytes }
    }
}

pub fn hash<T: IntoBoxed>(object: T) -> Result<[u8; 32]> {
    hash_boxed(&object.into_boxed())
}

pub fn build_packet_cipher(shared_secret: &[u8; 32], checksum: &[u8; 32]) -> aes::Aes256Ctr {
    use aes::cipher::NewCipher;

    let mut aes_key_bytes: [u8; 32] = *shared_secret;
    aes_key_bytes.copy_from_slice(&checksum[16..32]);
    let mut aes_ctr_bytes: [u8; 16] = checksum[0..16].try_into().unwrap();
    aes_ctr_bytes.copy_from_slice(&shared_secret[20..32]);

    aes::Aes256Ctr::new(
        generic_array::GenericArray::from_slice(&aes_key_bytes),
        generic_array::GenericArray::from_slice(&aes_ctr_bytes),
    )
}

pub fn compute_shared_secret(private_key: &[u8; 32], public_key: &[u8; 32]) -> Result<[u8; 32]> {
    let point = curve25519_dalek::edwards::CompressedEdwardsY(*public_key)
        .decompress()
        .ok_or(BadPublicKeyData)?
        .to_montgomery()
        .to_bytes();
    Ok(x25519_dalek::x25519(*private_key, point))
}

#[derive(thiserror::Error, Debug)]
#[error("Bad public key data")]
struct BadPublicKeyData;

pub fn hash_boxed<T: BoxedSerialize>(object: &T) -> Result<[u8; 32]> {
    let buf = sha2::Sha256::digest(&serialize(object)?);
    Ok(buf.as_slice().try_into().unwrap())
}

pub fn serialize<T: BoxedSerialize>(object: &T) -> Result<Vec<u8>> {
    let mut ret = Vec::new();
    Serializer::new(&mut ret).write_boxed(object).convert()?;
    Ok(ret)
}

pub trait NoFailure {
    type Output;

    fn convert(self) -> anyhow::Result<Self::Output>;
}

impl<T> NoFailure for ton_types::Result<T> {
    type Output = T;

    fn convert(self) -> anyhow::Result<Self::Output> {
        self.map_err(|e| anyhow::Error::msg(e.to_string()))
    }
}