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

use std::cmp::{self};
use std::collections::BinaryHeap;
use std::collections::binary_heap::PeekMut;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key()) // Compare keys
            .then(self.0.cmp(&other.0)) // Tie-breaker - based on index
            .reverse()

        // Reverse to make the smallest key come first in the heap
        // Smallest Key first, smallest index first on tie-break
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>, // Heap of iterators, with the head of the iterators being used for ordering in the heap.
    current: Option<HeapWrapper<I>>,   // Current element being iterated over.
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        if iters.is_empty() || iters.iter().all(|iter| !iter.is_valid()) {
            return Self {
                iters: BinaryHeap::new(),
                current: None,
            };
        }

        let mut heap = BinaryHeap::new();
        for (index, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(index, iter));
            }
        }

        let current = heap.pop().unwrap();
        Self {
            iters: heap,
            current: Some(current),
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map(|wrapper| wrapper.1.is_valid())
            .unwrap_or(false)
    }

    fn next(&mut self) -> Result<()> {
        let current_iterator = self.current.as_mut().unwrap();
        // Move to the next position in the top iterator available on the heap if they hold the same key as current iterator.
        while let Some(mut heap_top_iterator) = self.iters.peek_mut() {
            if heap_top_iterator.1.key() == current_iterator.1.key() {
                // Move to the next position in the top iterator.
                if let e @ Err(_) = heap_top_iterator.1.next() {
                    PeekMut::pop(heap_top_iterator);
                    return e;
                }

                // Reached the end of the iterator, remove it from the heap.
                if !heap_top_iterator.1.is_valid() {
                    PeekMut::pop(heap_top_iterator);
                }
            } else {
                break;
            }
        }
        // Update the current iterator
        current_iterator.1.next()?;

        // If the current iterator has reached the end
        if !current_iterator.1.is_valid() {
            // update with smallest iterator from the heap.
            if let Some(iter) = self.iters.pop() {
                *current_iterator = iter;
            }

            return Ok(());
        }
        // All the iterators will now have non-colliding keys and the top iterator or the current iterator has the smallest key.
        // Swap the current iterator with the top iterator in the heap if it is smaller.
        if let Some(mut inner_iter) = self.iters.peek_mut() {
            if *current_iterator < *inner_iter {
                std::mem::swap(&mut *inner_iter, current_iterator);
            }
        }
        Ok(())
    }
}
