// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use bytes::BufMut;

use crate::{
    block::SIZEOF_U16,
    key::{KeySlice, KeyVec},
};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    fn estimated_size(&self) -> usize {
        SIZEOF_U16 + self.offsets.len() * SIZEOF_U16 + self.data.len()
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    /// You may find the `bytes::BufMut` trait useful for manipulating binary data.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        assert!(!key.is_empty(), "Key must not be empty");

        let current_kv_size = key.len() + value.len() + SIZEOF_U16 * 3; // Key Len, Value Len, and Offset
        if self.estimated_size() + current_kv_size > self.block_size && self.offsets.len() != 0 {
            return false;
        }

        if self.is_empty() {
            self.first_key = KeyVec::from_vec(key.into_inner().to_vec());
        }
        // Offsets
        self.offsets.push(self.data.len() as u16);

        // Data
        self.data.put_u16(key.len() as u16); // Key length
        self.data.put(key.raw_ref()); // Key data
        self.data.put_u16(value.len() as u16); // Value length
        self.data.put(value); // Value data
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        if self.is_empty() {
            panic!("block should not be empty");
        }
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
