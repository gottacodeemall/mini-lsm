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

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{BufMut, Bytes};

use super::{BlockMeta, SsTable};
use crate::{block::BlockBuilder, key::{KeyBytes, KeySlice}, lsm_storage::BlockCache, table::FileObject};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            data: Vec::new(),
            meta: Vec::new(),
            first_key: Vec::new(),
            last_key: Vec::new(),
            block_size,
            builder: BlockBuilder::new(block_size),
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key.clear();
            self.first_key.extend(key.raw_ref());
        }

        if self.builder.add(key, value) {
            self.last_key.clear();
            self.last_key.extend(key.raw_ref());
            return;
        }

        // create a new block builder and append block data
        self.finish_block();

        // add the key-value pair to the next block
        assert!(self.builder.add(key, value));
        self.first_key.clear();
        self.first_key.extend(key.raw_ref());
        self.last_key.clear();
        self.last_key.extend(key.raw_ref());
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.finish_block();

        let mut encoded_sstable = self.data; // 1. Data blocks
        let meta_block_offset = encoded_sstable.len();
        BlockMeta::encode_block_meta(&self.meta, &mut encoded_sstable); // 2. Meta blocks
        encoded_sstable.put_u32(meta_block_offset as u32); // 3. Metadata block offset
        let file = FileObject::create(path.as_ref(), encoded_sstable)?;
        Ok(
            SsTable {
                first_key: self.meta.first().unwrap().first_key.clone(),
                last_key: self.meta.last().unwrap().last_key.clone(),
                file,
                block_meta: self.meta,
                block_meta_offset: meta_block_offset as usize,
                id,
                block_cache,
                bloom: None,
                max_ts: 0,
            }
        )
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }

    /// Adds the metadata and the data for the current block to the SSTable.
    fn finish_block(&mut self) {
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let encoded_block = builder.build().encode();
        self.meta.push(
            BlockMeta {
                offset: self.data.len(),
                first_key: KeyBytes::from_bytes(Bytes::from(self.first_key.clone())),
                last_key: KeyBytes::from_bytes(Bytes::from(self.last_key.clone())),
        });
        self.data.extend(encoded_block);
    }
}
