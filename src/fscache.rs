// BackFS Filesystem Cache
//
// Copyright (c) 2016 by William R. Fraser
//

use std::ffi::OsStr;
use std::fmt::Debug;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

use block_map::{CacheBlockMap, CacheBlockMapFileEntry, CacheBlockMapFileEntryResult};
use bucket_store::CacheBucketStore;

use libc;
use log;

pub struct FSCache<M: CacheBlockMap, S: CacheBucketStore> {
    map: M,
    store: S,
    block_size: u64,
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

impl<M: CacheBlockMap, S: CacheBucketStore> FSCache<M, S> {
    pub fn new(map: M, store: S, block_size: u64) -> FSCache<M, S> {
        FSCache {
            map: map,
            store: store,
            block_size: block_size,
        }
    }

    fn write_block_to_cache(&mut self, path: &OsStr, block: u64, data: &[u8],
                            entry: &mut CacheBlockMapFileEntry)
                            -> io::Result<()> {
        let bucket_path = match self.store.put(data) {
            Ok(path) => path,
            Err(e) => {
                if e.raw_os_error() != Some(libc::ENOSPC) {
                    error!("error writing data into cache: {}", e);
                }
                return Err(e);
            }
        };

        if let Err(e) = entry.put(block, &bucket_path) {
            error!("error mapping cache bucket {:?} to {:?}/{}", bucket_path, path, block);
            return Err(e);
        }
        Ok(())
    }
}

impl<M: CacheBlockMap, S: CacheBucketStore> Cache for FSCache<M, S> {
    fn init(&mut self) -> io::Result<()> {
        //TODO
        Ok(())
    }

    fn used_size(&self) -> u64 {
        self.store.used_bytes()
    }

    fn max_size(&self) -> io::Result<u64> {
        match self.store.max_bytes() {
            //None => self.get_fs_size(&self.buckets_dir)
            None => Ok(1), // TODO!!
            Some(n) => Ok(n)
        }
    }

    fn invalidate_path<T: AsRef<Path> + ?Sized + Debug>(&mut self, path: &T) -> io::Result<()> {
        let path: &Path = path.as_ref();
        debug!("invalidate_path: {:?}", path);
        let store = &mut self.store;
        self.map.invalidate(path.as_os_str(), |bucket_path| {
            match store.delete(&bucket_path) {
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
        let mut file_entry: Box<CacheBlockMapFileEntry> = match self.map.get_file_entry(path, mtime) {
            Ok(CacheBlockMapFileEntryResult::Entry(entry)) => entry,
            Ok(CacheBlockMapFileEntryResult::StaleDataPresent) => {
                info!("invalidating stale data for {:?}", path);
                if let Err(e) = self.invalidate_path(path) {
                    error!("unable to invalidate {:?}: {}", path, e);
                    return Err(e);
                }
                if let Ok(CacheBlockMapFileEntryResult::Entry(entry)) = self.map.get_file_entry(path, mtime) {
                    entry
                } else {
                    panic!("still couldn't get an entry after invalidation!");
                }
            }
            Err(e) => {
                error!("error getting a map file entry for {:?}: {}", path, e);
                return Err(e);
            }
        };

        let first_block = offset / self.block_size;
        let last_block = (offset + size - 1) / self.block_size;

        debug!("fetching blocks {} to {} from {:?}", first_block, last_block, path);

        let mut result: Vec<u8> = Vec::with_capacity(size as usize);

        for block in first_block..(last_block + 1) {
            debug!("fetching block {}", block);

            let mut block_data: Vec<u8> = match file_entry.get(block) {
                Ok(Some(bucket_path)) => {
                    match self.store.get(&bucket_path) {
                        Ok(data) => {
                            info!("cache hit: got {:#x} to {:#x} from {:?}",
                                  block * self.block_size,
                                  block * self.block_size + data.len() as u64,
                                  path);
                            data
                        },
                        Err(e) => {
                            error!("error reading cached data for block {} of {:?}: {}", block, path, e);
                            return Err(e);
                        }
                    }
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

                    while let Err(e) = self.write_block_to_cache(path, block, &buf, &mut *file_entry) {
                        if e.raw_os_error().unwrap() == libc::ENOSPC {
                            info!("writing to cache failed; freeing some space");
                            match self.store.delete_something() {
                                Ok((bucket_path, n)) => {
                                    debug!("freed {} bytes from {:?}", n, bucket_path);
                                    if let Err(e) = self.map.delete(&bucket_path) {
                                        error!("error removing bucket {:?} from the map", bucket_path);
                                        return Err(e);
                                    }
                                },
                                Err(e) => {
                                    error!("error freeing space for cache data: {}", e);
                                    return Err(e);
                                }
                            }
                        } else {
                            error!("unhandled error writing to cache: {}", e);
                            break;
                        }
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
