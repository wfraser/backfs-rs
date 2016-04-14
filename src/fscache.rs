// BackFS Filesystem Cache
//
// Copyright (c) 2016 by William R. Fraser
//

use std::borrow::{Borrow, BorrowMut};
use std::ffi::OsStr;
use std::fmt::Debug;
use std::io::{self, Read, Seek, SeekFrom};
use std::marker::PhantomData;
use std::path::Path;

use block_map::{CacheBlockMap, CacheBlockMapFileResult};
use bucket_store::CacheBucketStore;

use log;

pub struct FSCache<M, S, X1, X2> {
    map: M,
    store: S,
    block_size: u64,
    phantom1: PhantomData<X1>,
    phantom2: PhantomData<X2>
}

macro_rules! log2 {
    ($lvl:expr, $($arg:tt)+) => (
        log!(target: "FSCache", $lvl, $($arg)+));
}

macro_rules! error {
    ($($arg:tt)+) => (log2!(log::LogLevel::Error, $($arg)+));
}

macro_rules! warn {
    ($($arg:tt)+) => (log2!(log::LogLevel::Warn, $($arg)+));
}

macro_rules! info {
    ($($arg:tt)+) => (log2!(log::LogLevel::Info, $($arg)+));
}

macro_rules! debug {
    ($($arg:tt)+) => (log2!(log::LogLevel::Debug, $($arg)+));
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
    fn init(&mut self) -> io::Result<()>;
    fn used_size(&self) -> u64;
    fn max_size(&self) -> io::Result<u64>;
    fn invalidate_path<T: AsRef<Path> + ?Sized + Debug>(&mut self, path: &T) -> io::Result<()>;
    fn free_orphaned_buckets(&mut self) -> io::Result<()>;
    fn fetch<F>(&mut self, path: &OsStr, offset: u64, size: u64, file: &mut F, mtime: i64)
        -> io::Result<Vec<u8>>
        where F: Read + Seek;
}

impl<M, S, X1, X2> FSCache<M, S, X1, X2>
        where M: BorrowMut<X1>, X1: CacheBlockMap,
              S: BorrowMut<X2>, X2: CacheBucketStore {
    pub fn new(map: M, store: S, block_size: u64) -> FSCache<M, S, X1, X2> {
        FSCache {
            map: map,
            store: store,
            block_size: block_size,
            phantom1: PhantomData,
            phantom2: PhantomData,
        }
    }

    fn try_get_cached_block(&mut self, path: &OsStr, block: u64) -> io::Result<Option<Vec<u8>>> {
        let bucket_path = match self.map.borrow().get_block(path, block) {
            Ok(Some(bucket_path)) => bucket_path,
            Ok(None) => {
                return Ok(None)
            },
            Err(e) => {
                error!("failed to get bucket path for block {} of {:?}: {}", block, path, e);
                return Err(e);
            }
        };

        match self.store.borrow().get(&bucket_path) {
            Ok(data) => Ok(Some(data)),
            Err(e) => {
                error!("error reading cached data for block {} of {:?}: {}", block, path, e);
                Err(e)
            }
        }
    }

    fn write_block_into_cache(&mut self, path: &OsStr, block: u64, data: &[u8]) -> io::Result<()> {
        assert!(data.len() > 0);
        let map = self.map.borrow_mut();
        let map_path = map.get_block_path(path, block);
        let bucket_path = trylog!(self.store.borrow_mut().put(&map_path, data, |map_path| map.unmap_block(map_path).and(Ok(()))),
                                  "failed to write to cache");
        trylog!(map.put_block(path, block, &bucket_path),
                "failed to map bucket {:?} into map for block {:?}/{}", bucket_path, path, block);
        Ok(())
    }
}

impl<M, S, X1, X2> Cache for FSCache<M, S, X1, X2>
        where M: BorrowMut<X1>, X1: CacheBlockMap,
              S: BorrowMut<X2>, X2: CacheBucketStore {
    fn init(&mut self) -> io::Result<()> {
        let map = self.map.borrow_mut();
        self.store.borrow_mut().init(|map_path| map.unmap_block(map_path))
    }

    fn used_size(&self) -> u64 {
        self.store.borrow().used_bytes()
    }

    fn max_size(&self) -> io::Result<u64> {
        match self.store.borrow().max_bytes() {
            //None => self.get_fs_size(&self.buckets_dir)
            None => Ok(1), // TODO!!
            Some(n) => Ok(n)
        }
    }

    fn invalidate_path<T: AsRef<Path> + ?Sized + Debug>(&mut self, path: &T) -> io::Result<()> {
        let path: &Path = path.as_ref();
        debug!("invalidate_path: {:?}", path);
        let store = self.store.borrow_mut();
        self.map.borrow_mut().invalidate_path(path.as_os_str(), |ref bucket_path| {
            match store.free_bucket(&bucket_path) {
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

    fn free_orphaned_buckets(&mut self) -> io::Result<()> {
        debug!("free_orphaned_buckets");
        unimplemented!();
    }

    fn fetch<F>(&mut self, path: &OsStr, offset: u64, size: u64, file: &mut F, mtime: i64)
            -> io::Result<Vec<u8>>
            where F: Read + Seek {

        let freshness = trylog!(self.map.borrow().check_file_mtime(path, mtime),
                                "error checking cache freshness for {:?}", path);

        if freshness == CacheBlockMapFileResult::Stale {
            info!("cache data for {:?} is stale; invalidating", path);
            let store = self.store.borrow_mut();
            trylog!(self.map.borrow_mut().invalidate_path(path, |bucket_path| store.free_bucket(bucket_path).and(Ok(()))),
                    "failed to invalidate stale cache data for {:?}", path);
        }

        if freshness != CacheBlockMapFileResult::Current {
            try!(self.map.borrow_mut().set_file_mtime(path, mtime));
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
                    try!(file.seek(SeekFrom::Start(block * self.block_size)));

                    let nread = try!(file.read(&mut buf[..])) as u64;
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
            } else {
                if block == first_block && block == last_block {
                    // Optimization for the common case where we read exactly 1 block.
                    return Ok(block_data);
                } else {
                    // Take the whole block and add it to the result set.
                    result.extend(block_data.drain(..));
                }
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
}
