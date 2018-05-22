// BackFS Filesystem Cache
//
// Copyright 2016-2018 by William R. Fraser
//

use std::borrow::BorrowMut;
use std::ffi::OsStr;
use std::fmt::Debug;
use std::io::{self, Read, Seek, SeekFrom};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use block_map::{CacheBlockMap, CacheBlockMapFileResult};
use bucket_store::CacheBucketStore;

// FSCache has two generic parameters for each of the block map and the bucket store.
// The {Map, Store} parameters are for a type that can be borrowed to give an implementation of
// the map and store traits, and {MapImpl, StoreImpl} are the concrete types that implement the
// traits. In normal usage these are the exact same type, but for test mocking purposes, the one
// implementing borrow may be different.
// Ideally we wouldn't have to explicitly specify the implementation type, and could instead have
// {Map, Store} be bounded on `BorrowMut<WhateverTrait>` directly, but because the map and block
// traits both have functions with generic parameters themselves, Rust won't let you make a trait
// object out of them, and so we have to explicitly parameterize over them. :(
pub struct FSCache<Map, MapImpl, Store, StoreImpl> {
    map: RwLock<Map>,
    store: RwLock<Store>,
    block_size: u64,
    _p1: PhantomData<MapImpl>,
    _p2: PhantomData<StoreImpl>,
}

macro_rules! trylog {
    ($e:expr, $fmt:expr) => {
        match $e {
            Ok(x) => x,
            Err(e) => {
                error!(concat!($fmt, ": {}\n"), e);
                return Err(e);
            }
        }
    };
    ($e:expr, $fmt:expr, $($arg:tt)*) => {
        match $e {
            Ok(x) => x,
            Err(e) => {
                error!(concat!($fmt, ": {}\n"), $($arg)*, e);
                return Err(e);
            },
        }
    }
}

pub trait Cache {
    fn init(&self) -> io::Result<()>;
    fn used_size(&self) -> u64;
    fn max_size(&self) -> Option<u64>;
    fn invalidate_path<T: AsRef<Path> + ?Sized + Debug>(&self, path: &T) -> io::Result<()>;
    fn free_orphaned_buckets(&self) -> io::Result<()>;
    fn fetch<F>(&self, path: &OsStr, offset: u64, size: u64, file: &mut F, mtime: i64)
        -> io::Result<Vec<u8>>
        where F: Read + Seek;
    fn count_cached_bytes(&self, path: &OsStr) -> u64;
}

impl<Map, MapImpl, Store, StoreImpl> FSCache<Map, MapImpl, Store, StoreImpl>
where
    Map: BorrowMut<MapImpl>,
    MapImpl: CacheBlockMap,
    Store: BorrowMut<StoreImpl>,
    StoreImpl: CacheBucketStore,
{
    pub fn new(map: Map, store: Store, block_size: u64)
        -> FSCache<Map, MapImpl, Store, StoreImpl>
    {
        FSCache {
            map: RwLock::new(map),
            store: RwLock::new(store),
            block_size,
            _p1: PhantomData,
            _p2: PhantomData,
        }
    }

    fn try_get_cached_block(&self, path: &OsStr, block: u64) -> io::Result<Option<Vec<u8>>> {
        let map = self.map.read().unwrap();
        let store = self.store.read().unwrap();

        let bucket_path = match (*map).borrow().get_block(path, block) {
            Ok(Some(bucket_path)) => bucket_path,
            Ok(None) => {
                return Ok(None)
            },
            Err(e) => {
                error!("failed to get bucket path for block {} of {:?}: {}", block, path, e);
                return Err(e);
            }
        };

        match (*store).borrow().get(&bucket_path) {
            Ok(data) => Ok(Some(data)),
            Err(e) => {
                error!("error reading cached data for block {} of {:?}: {}", block, path, e);
                Err(e)
            }
        }
    }

    fn write_block_into_cache(&self, path: &OsStr, block: u64, data: &[u8]) -> io::Result<()> {
        assert!(!data.is_empty());
        let mut map = self.map.write().unwrap();
        let mut store = self.store.write().unwrap();

        let map_path = (*map).borrow_mut().get_block_path(path, block);
        let bucket_path = trylog!(
            (*store).borrow_mut().put(&map_path, data, |map_path| (*map)
                .borrow_mut()
                .unmap_block(map_path)
                .and(Ok(()))),
            "failed to write to cache");
        trylog!(
            (*map).borrow_mut().put_block(path, block, &bucket_path),
            "failed to map bucket {:?} into map for block {:?}/{}",
            bucket_path, path, block);
        Ok(())
    }

    pub fn free_block(&self, path: &OsStr, block: u64)
        -> io::Result<Option<u64>>
    {
        debug!("free_block({:?}, {})", path, block);
        let mut map = self.map.write().unwrap();
        let mut store = self.store.write().unwrap();
        let block_path = (*map).borrow().get_block_path(path, block);
        let bucket_path = (*map).borrow().get_block(path, block)?;
        if let Some(bucket_path) = bucket_path {
            let freed = (*store).borrow_mut().free_bucket(&bucket_path);
            (*map).borrow_mut().unmap_block(&block_path)?;
            freed.map(Some)
        } else {
            Ok(None)
        }
    }
}

impl<Map, MapImpl, Store, StoreImpl> Cache for FSCache<Map, MapImpl, Store, StoreImpl>
where
    Map: BorrowMut<MapImpl>,
    MapImpl: CacheBlockMap,
    Store: BorrowMut<StoreImpl>,
    StoreImpl: CacheBucketStore,
{
    fn init(&self) -> io::Result<()> {
        let mut map = self.map.write().unwrap();
        let mut store = self.store.write().unwrap();
        (*store).borrow_mut().init(|map_path| (*map).borrow_mut().unmap_block(map_path))
    }

    fn used_size(&self) -> u64 {
        (*self.store.read().unwrap()).borrow().used_bytes()
    }

    fn max_size(&self) -> Option<u64> {
        (*self.store.read().unwrap()).borrow().max_bytes()
    }

    fn invalidate_path<T: AsRef<Path> + ?Sized + Debug>(&self, path: &T) -> io::Result<()> {
        let path: &Path = path.as_ref();
        debug!("invalidate_path: {:?}", path);
        let mut store = self.store.write().unwrap();
        (*self.map.write().unwrap())
            .borrow_mut()
            .invalidate_path(path.as_os_str(), |bucket_path| {
                match (*store).borrow_mut().free_bucket(bucket_path) {
                    Ok(n) => {
                        info!("freed {} bytes from bucket {:?}", n, bucket_path);
                        Ok(())
                    },
                    Err(e) => {
                        error!("error freeing bucket {:?}: {}", bucket_path, e);
                        Err(e)
                    }
                }
            })
    }

    fn free_orphaned_buckets(&self) -> io::Result<()> {
        debug!("free_orphaned_buckets");

        let mut orphans: Vec<PathBuf> = vec![];

        {
            let map_read = self.map.read().unwrap();
            let store_read = self.store.read().unwrap();
            store_read.borrow().enumerate_buckets(
                |bucket_path, parent_opt| {
                    if let Some(parent) = parent_opt {
                        if !(*map_read).borrow().is_block_mapped(parent)? {
                            warn!("bucket {:?} is an orphan; it was parented to {:?}",
                                  bucket_path, parent);
                            orphans.push(PathBuf::from(bucket_path));
                        }
                    }
                    Ok(())
                }
            )?;
        }

        if !orphans.is_empty() {
            let mut store_write = self.store.write().unwrap();
            for bucket in orphans {
                (*store_write).borrow_mut().free_bucket(bucket.as_os_str())?;
            }
        }

        Ok(())
    }

    #[allow(cyclomatic_complexity)] // FIXME: split this up into smaller pieces
    fn fetch<F>(&self, path: &OsStr, offset: u64, size: u64, file: &mut F, mtime: i64)
            -> io::Result<Vec<u8>>
            where F: Read + Seek {

        let freshness = {
            trylog!((*self.map.read().unwrap()).borrow().check_file_mtime(path, mtime),
                    "error checking cache freshness for {:?}", path)
        };

        if freshness == CacheBlockMapFileResult::Stale {
            info!("cache data for {:?} is stale; invalidating", path);
            let mut store = self.store.write().unwrap();
            let mut map = self.map.write().unwrap();
            trylog!(
                (*map).borrow_mut().invalidate_path(
                    path,
                    |bucket_path| (*store).borrow_mut().free_bucket(bucket_path).and(Ok(()))
                ),
                "failed to invalidate stale cache data for {:?}", path);
        }

        if freshness != CacheBlockMapFileResult::Current {
            // TODO: make a macro for this type of retry loop
            let mut store = self.store.write().unwrap();
            let mut map = self.map.write().unwrap();
            while let Err(e) = (*map).borrow_mut().set_file_mtime(path, mtime) {
                if e.raw_os_error() == Some(::libc::ENOSPC) {
                    (*store).borrow_mut().delete_something()?;
                } else {
                    error!("failed to set mtime file {:?}: {}", path, e);
                    return Err(e);
                }
            }
        }

        let first_block = offset / self.block_size;
        let last_block = (offset + size - 1) / self.block_size;

        debug!("fetching blocks {} to {} from {:?}", first_block, last_block, path);

        let mut result: Vec<u8> = Vec::with_capacity(size as usize);

        for block in first_block..(last_block + 1) {
            debug!("fetching block {}", block);

            let mut block_data: Vec<u8> = match self.try_get_cached_block(path, block) {
                Ok(Some(data)) => {
                    info!("cache hit: got {:#x} to {:#x} from {:?}",
                          block * self.block_size,
                          block * self.block_size + data.len() as u64,
                          path);
                    data
                },
                Ok(None) => {
                    info!("cache miss: reading {:#x} to {:#x} from {:?}",
                          block * self.block_size,
                          (block + 1) * self.block_size,
                          path);

                    // TODO: try to write into a slice of `result` in place instead of writing to
                    // a new buffer and moving the data later.

                    let mut buf: Vec<u8> = Vec::with_capacity(self.block_size as usize);
                    unsafe {
                        buf.set_len(self.block_size as usize);
                    }

                    // TODO: skip this when doing contiguous reads from the file
                    file.seek(SeekFrom::Start(block * self.block_size))?;

                    let nread = file.read(&mut buf[..])? as u64;
                    debug!("read {:#x} bytes", nread);

                    if nread != self.block_size {
                        buf.truncate(nread as usize);
                    }

                    if nread > 0 {
                        trylog!(self.write_block_into_cache(path, block, &buf),
                                "unhandled error writing to cache");
                    }

                    buf
                },
                Err(e) => {
                    error!("error getting bucket path for block {} of {:?}: {}", block, path, e);
                    return Err(e);
                }
            };

            let nread = block_data.len() as u64;

            let block_start = if block == first_block {
                // read starts part-way into this block
                offset - block * self.block_size
            } else {
                0
            };

            let mut block_end = if block == last_block {
                // read ends part-way into this block
                (offset + size) - (block * self.block_size)
            } else {
                self.block_size
            };

            if block_end == 0 {
                continue;
            }

            if nread < block_end {
                // we read less than expected
                block_end = nread;
            }

            debug!("block_start({:#x}) block_end({:#x}) nread({:#x})",
                 block_start, block_end, nread);

            if block_start > block_end {
                warn!("block_start({:#x}) > block_end({:#x}): on read {:#x} @ {:#x} (block {}, nread = {:#x})",
                      block_start, block_end, size, offset, block, nread);
                // Return an empty result. This is the expected behavior when a client seeks past
                // the end of a file (not an error) and does a read.
                return Ok(vec![]);
            }

            if block_start != 0 || block_end != nread {
                // read a slice of the block
                result.extend(&block_data[block_start as usize .. block_end as usize]);
            } else if block == first_block && block == last_block {
                // Optimization for the common case where we read exactly 1 block.
                return Ok(block_data);
            } else {
                // Take the whole block and add it to the result set.
                result.extend(block_data.drain(..));
            }

            if nread < self.block_size {
                // if we read less than requested, we're done.
                if block < last_block {
                    warn!("read fewer blocks than requested from {:?}", path);
                }
                break;
            }
        } // for block

        Ok(result)
    }

    fn count_cached_bytes(&self, path: &OsStr) -> u64 {
        let mut sum = 0;
        let map = self.map.read().unwrap();
        let store = self.store.read().unwrap();
        if let Err(e) = (*map).borrow().for_each_block_under_path(path, |block_path| {
            sum += (*store).borrow().get_size(block_path)?;
            Ok(())
        }) {
            error!("failed to count cached bytes under {:?}: {}", path, e);
            return 0;
        }
        sum
    }
}
