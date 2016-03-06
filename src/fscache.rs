// BackFS Filesystem Cache
//
// Copyright (c) 2016 by William R. Fraser
//

use std::ffi::{CString, OsStr, OsString};
use std::fmt;
use std::fs;
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::MetadataExt;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use libc::*;

pub struct FSCache {
    cache_dir: OsString,
    buckets_dir: OsString,
    map_dir: OsString,
    block_size: u64,
    used_bytes: u64,
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
            buckets_dir: OsString::from(String::from(cache) + "/buckets"),
            map_dir: OsString::from(String::from(cache) + "/map"),
            block_size: block_size,
            used_bytes: 0u64,
            debug: false,
        }
    }

    fn create_dir_and_check_access(&self, pathstr: &OsStr) -> io::Result<()> {
        let path = Path::new(pathstr);
        if let Err(e) = fs::create_dir(&path) {
            // Already existing is fine.
            if e.raw_os_error() != Some(EEXIST) {
                log!(self, "error: unable to create {:?}: {}", pathstr, e);
                return Err(e);
            }
        }

        // Check for read, write, and execute permissions on the folder.
        // This doesn't 100% guarantee things will work, but it will catch most common problems
        // early, so it's still worth doing.
        unsafe {
            // safe because it can't have NUL bytes if we already got this far...
            let path_c = CString::from_vec_unchecked(Vec::from(pathstr.as_bytes()));
            if 0 != access(path_c.as_ptr(), R_OK | W_OK | X_OK) {
                let e = io::Error::last_os_error();
                log!(self, "error: no R/W/X access to {:?}: {}", pathstr, e);
                return Err(e);
            }
        }

        Ok(())
    }

    pub fn init(&mut self) -> io::Result<()> {
        try!(self.create_dir_and_check_access(&self.buckets_dir));
        try!(self.create_dir_and_check_access(&self.map_dir));

        // TODO: check and/or write 'bucket_size' marker file

        // TODO: initialize FSLL

        self.used_bytes = try!(self.get_cache_used_size());

        Ok(())
    }

    fn get_cache_used_size(&mut self) -> io::Result<u64> {
        let mut size = 0u64;

        let readdir = match fs::read_dir(Path::new(&self.buckets_dir)) {
            Ok(readdir) => readdir,
            Err(e) => {
                log!(self, "error getting directory listing for bucket directory: {}", e);
                return Err(e);
            }
        };

        for entry_result in readdir {
            let entry = match entry_result {
                Ok(entry) => entry,
                Err(e) => {
                    log!(self, "error reading directory entry: {}", e);
                    return Err(e);
                }
            };

            match entry.file_type() {
                Ok(ft) => {
                    if !ft.is_dir() {
                        continue;
                    }
                },
                Err(e) => {
                    log!(self, "error getting file type of {:?}: {}",
                               entry.file_name(), e);
                    return Err(e);
                }
            }

            let bucket_number: u64 = match entry.file_name().to_str() {
                Some(name) => {
                    match name.parse::<u64>() {
                        Ok(n) => n, // folder name parses as a number: consider it a bucket.
                        Err(_) => { continue; }
                    }
                },
                None => { continue; }
            };

            let mut path: OsString = self.buckets_dir.clone();
            path.push(&OsString::from("/"));
            path.push(entry.file_name());
            path.push(&OsString::from("/data"));
            log!(self, "checking {:?}", path);

            let len = match fs::File::open(&path) {
                Ok(file) => {
                    match file.metadata() {
                        Ok(metadata) => {
                            metadata.len()
                        },
                        Err(e) => {
                            log!(self, "failed to get data file metadata: {}", e);
                            return Err(e);
                        }
                    }
                },
                Err(e) => {
                    log!(self, "failed to open data file: {}", e);
                    return Err(e);
                }
            };

            log!(self, "bucket {}: {} bytes", bucket_number, len);

            size += len;
        }

        log!(self, "cache used size: {} bytes", size);

        Ok(size)
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

    pub fn fetch(&self, path: &OsStr, offset: u64, size: u64, file: &mut fs::File) -> io::Result<Vec<u8>> {
        let mtime = try!(file.metadata()).mtime() as u64;

        if self.cached_mtime(path) != mtime {
            self.invalidate_path(path);
        }

        let first_block = offset / self.block_size;
        let last_block  = (offset + size - 1) / self.block_size;

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
