use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::File;
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::MetadataExt;

pub struct FSCache {
    cache_dir: OsString,
    block_size: u64,
    pub debug: bool,
}

macro_rules! log {
    ($s:expr, $fmt:expr) => ($s.log(format_args!($fmt)));
    ($s:expr, $fmt:expr, $($arg:tt)*) => ($s.log(format_args!($fmt, $($arg)*)));
}

impl FSCache {
    pub fn new(cache: &str, block_size: u64) -> FSCache {
        FSCache {
            cache_dir: OsString::from(cache),
            block_size: block_size,
            debug: false,
        }
    }

    fn log(&self, args: fmt::Arguments) {
        if self.debug {
            println!("FSCache: {}", fmt::format(args));
        }
    }

    fn cached_block(&self, _path: &OsStr, _block: u64) -> Option<Vec<u8>> {
        // TODO: look up block in cache
        // for now, always cache miss
        None
    }
    
    fn cached_mtime(&self, _path: &OsStr) -> u64 {
        // TODO
        0
    }

    fn write_block_to_cache(&self, _path: &OsStr, _block: u64, _data: &[u8], _mtime: u64) {
        // TODO
    }

    pub fn invalidate_path(&self, _path: &OsStr) {
        // TODO
    }

    pub fn fetch(&self, path: &OsStr, offset: u64, size: u64, file: &mut File) -> io::Result<Vec<u8>> {
        let mtime = try!(file.metadata()).mtime() as u64;

        if self.cached_mtime(path) != mtime {
            self.invalidate_path(path);
        }

        let first_block = offset / self.block_size;
        let last_block  = (offset + size) / self.block_size - 1;

        log!(self, "fetching blocks {} to {} from {}", first_block, last_block, path.to_string_lossy());

        if first_block != 0 {
            try!(file.seek(SeekFrom::Start(first_block * self.block_size)));
        }

        let mut result: Vec<u8> = Vec::with_capacity(size as usize);

        for block in first_block..(last_block + 1) {
            log!(self, "fetching block {}", block);

            let mut block_data = match self.cached_block(path, block) {
                Some(data) => {
                    log!(self, "cache hit");
                    data
                },
                None => {
                    log!(self, "cache miss: reading {} to {} from real file", block * self.block_size, (block + 1) * self.block_size);
                    let mut buf: Vec<u8> = Vec::with_capacity(self.block_size as usize);
                    unsafe {
                        buf.set_len(self.block_size as usize);
                    }

                    let nread = try!(file.read(&mut buf[..])) as u64;
                    log!(self, "read {} bytes", nread);

                    if nread != self.block_size {
                        buf.truncate(nread as usize);
                    }

                    // TODO: write block back to cache
                    self.write_block_to_cache(path, block, &buf, mtime);

                    buf
                }
            };

            let nread = block_data.len() as u64;

            let block_offset = if block == first_block {
                // read starts part-way into this block
                offset - block * self.block_size
            } else {
                0
            };

            let mut block_size = if block == last_block {
                // read ends part-way into this block
                (offset + size) - (block * self.block_size) - block_offset
            } else {
                self.block_size - block_offset
            };

            if block_size == 0 {
                continue;
            }

            if nread < block_size as u64 {
                // we read less than requested
                block_size = nread;
            }

            if block_offset != 0 || block_size != nread {
                // read a slice of the block
                result.extend(&block_data[block_offset as usize .. block_size as usize]);
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
                    log!(self, "warning: read fewer blocks than requested");
                }
                break;
            }
        }

        Ok(result)
    }
}
