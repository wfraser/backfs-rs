// BackFS Filesystem Cache Block Store
//
// Copyright (c) 2016 by William R. Fraser
//

use std::ffi::{OsStr, OsString};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use fsll::PathLinkedList;
use utils;

use libc;
use log;

pub trait CacheBucketStore {
    fn get(&self, bucket_path: &OsStr) -> io::Result<Vec<u8>>;
    fn put(&mut self, data: &[u8]) -> io::Result<OsString>;
    fn free_bucket(&mut self, bucket_path: &OsStr) -> io::Result<u64>;
    fn delete_something(&mut self) -> io::Result<(OsString, u64)>;
    fn used_bytes(&self) -> u64;
    fn max_bytes(&self) -> Option<u64>;
}

pub struct FSCacheBucketStore<LL: PathLinkedList> {
    buckets_dir: OsString,
    used_list: LL,
    free_list: LL,
    used_bytes: u64,
    max_bytes: Option<u64>,
    bucket_size: u64,
    next_bucket_number: u64,
}

macro_rules! log2 {
    ($lvl:expr, $($arg:tt)+) => (
        log!(target: "BucketStore", $lvl, $($arg)+));
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

impl<LL: PathLinkedList> FSCacheBucketStore<LL> {
    pub fn new(buckets_dir: OsString, used_list: LL, free_list: LL, block_size: u64, max_bytes: Option<u64>)
            -> io::Result<FSCacheBucketStore<LL>> {
        let mut store = FSCacheBucketStore {
            buckets_dir: buckets_dir,
            used_list: used_list,
            free_list: free_list,
            used_bytes: 0,
            max_bytes: max_bytes,
            bucket_size: block_size,
            next_bucket_number: 0,
        };
        try!(store.init());
        Ok(store)
    }

    fn init(&mut self) -> io::Result<()> {
        self.next_bucket_number = try!(self.read_next_bucket_number());
        info!("next bucket number: {}", self.next_bucket_number);

        match utils::read_number_file(&PathBuf::from(&self.buckets_dir).join("bucket_size"),
                                      Some(self.bucket_size)) {
            Ok(Some(size)) => {
                if size != self.bucket_size {
                    let msg = format!(
                        "block size in cache ({}) doesn't match the size in the options ({})",
                        size,
                        self.bucket_size);
                    error!("{}", msg);
                    return Err(io::Error::new(io::ErrorKind::Other, msg));
                }
            },
            Err(e) => {
                let msg = format!("error reading bucket_size file: {}", e);
                error!("{}", msg);
                return Err(io::Error::new(io::ErrorKind::Other, msg));
            },
            Ok(None) => unreachable!()
        }

        self.used_bytes = try!(self.compute_cache_used_size());

        if self.max_bytes.is_some() && self.used_bytes > self.max_bytes.unwrap() {
            warn!("cache is over-size; freeing buckets until it is within limits");
            while self.used_bytes > self.max_bytes.unwrap() {
                try!(self.delete_something());
            }
        }

        Ok(())
    }

    fn read_next_bucket_number(&self) -> io::Result<u64> {
        let path = PathBuf::from(&self.buckets_dir).join("next_bucket_number");
        utils::read_number_file(&path, Some(0u64)).and_then(|r| Ok(r.unwrap()))
    }

    fn write_next_bucket_number(&self, bucket_number: u64) -> io::Result<()> {
        let path = PathBuf::from(&self.buckets_dir).join("next_bucket_number");
        utils::write_number_file(&path, bucket_number)
    }

    fn compute_cache_used_size(&mut self) -> io::Result<u64> {
        let mut size = 0u64;

        let readdir = trylog!(fs::read_dir(Path::new(&self.buckets_dir)),
                              "error getting directory listing for bucket directory");

        for entry_result in readdir {
            let entry = trylog!(entry_result, "error reading directory entry");
            let filetype = trylog!(entry.file_type(),
                                   "error getting file type of {:?}", entry.file_name());
            if !filetype.is_dir() {
                continue;
            }

            match entry.file_name().to_str().and_then(|name| Some(name.parse::<u64>())) {
                Some(Err(_)) | None => {
                    // folder name doesn't parse as a number: must not be a bucket. Skip it.
                    continue;
                },
                Some(Ok(_)) => (),
            }

            let mut path: OsString = self.buckets_dir.clone();
            path.push(&OsString::from("/"));
            path.push(entry.file_name());
            path.push(&OsString::from("/data"));

            let len = match fs::File::open(&path) {
                Ok(file) => {
                    trylog!(file.metadata().and_then(|m| Ok(m.len())),
                            "failed to get data file metadata from {:?}", path)
                },
                Err(e) => {
                    if e.raw_os_error() == Some(libc::ENOENT) {
                        0
                    } else {
                        error!("failed to open data file {:?}: {}", path, e);
                        return Err(e);
                    }
                }
            };

            size += len;
        }

        info!("cache used size: {} bytes", size);

        Ok(size)
    }

    fn get_bucket(&mut self) -> io::Result<PathBuf> {
        if self.free_list.is_empty() {
            debug!("making a new bucket");
            self.new_bucket()
        } else {
            let free_bucket: PathBuf = self.free_list.get_tail().unwrap();
            debug!("re-using free bucket {:?}", free_bucket);
            try!(self.free_list.disconnect(&free_bucket));
            try!(self.used_list.insert_as_head(&free_bucket));
            Ok(free_bucket)
        }
    }

    fn new_bucket(&mut self) -> io::Result<PathBuf> {
        let bucket_path = PathBuf::from(&self.buckets_dir).join(format!("{}", self.next_bucket_number));
        trylog!(fs::create_dir(&bucket_path),
                "error creating bucket directory {:?}", bucket_path);
        self.next_bucket_number += 1;
        trylog!(self.write_next_bucket_number(self.next_bucket_number),
                "error writing next bucket number");
        trylog!(self.used_list.insert_as_head(&bucket_path),
                "error setting bucket as head of used list");
        Ok(bucket_path)
    }

    fn free_bytes_needed_for_write(&self, size: u64) -> u64 {
        if self.max_bytes.is_none() || self.used_bytes + size <= self.max_bytes.unwrap() {
            0
        } else {
            self.used_bytes + size - self.max_bytes.unwrap()
        }
    }
}

impl<LL: PathLinkedList> CacheBucketStore for FSCacheBucketStore<LL> {
    fn get(&self, bucket_path: &OsStr) -> io::Result<Vec<u8>> {
        trylog!(self.used_list.to_head(bucket_path),
                "Error promoting bucket {:?} to head", bucket_path);

        let data_path = PathBuf::from(bucket_path).join("data");
        let mut block_file: File = trylog!(File::open(&data_path),
            "cached_block error opening bucket data file {:?}", data_path);;

        let mut data: Vec<u8> = Vec::with_capacity(self.bucket_size as usize);
        match block_file.read_to_end(&mut data) {
            Ok(nread) => {
                debug!("cached_block: read {:#x} bytes from cache", nread);
                Ok(data)
            },
            Err(e) => {
                warn!("cached_block reading from data file {:?}: {}", data_path, e);
                Err(e)
            }
        }
    }

    fn put(&mut self, data: &[u8]) -> io::Result<OsString> {
        loop {
            let bytes_needed = self.free_bytes_needed_for_write(data.len() as u64);
            if bytes_needed > 0 {
                info!("need to free {} bytes", bytes_needed);
                trylog!(self.delete_something(),
                        "error freeing up space");
            } else {
                break;
            }
        }

        let bucket_path: PathBuf = trylog!(self.get_bucket(),
                                           "error getting bucket");

        let data_path = bucket_path.join("data");

        let mut error: Option<io::Error> = None;
        let need_to_free_bucket = match OpenOptions::new()
                                                    .write(true)
                                                    .create(true)
                                                    .open(&data_path) {
            Ok(mut file) => {
                match file.write_all(data) {
                    Ok(()) => {
                        false
                    },
                    Err(e) => {
                        error!("error writing to cache data file: {}", e);
                        error = Some(e);
                        true
                    }
                }
            },
            Err(e) => {
                error!("put: error opening data file {:?}: {}", data_path, e);
                error = Some(e);
                true
            }
        };

        if need_to_free_bucket {
            // Something went wrong; we're not going to use this bucket.
            // Remove the data file first, so that `delete` doesn't try to count its size
            // (we haven't counted it in `used_bytes` yet).
            fs::remove_file(data_path).unwrap();

            // Return this empty bucket to the free list.
            self.free_bucket(bucket_path.as_os_str()).unwrap();
        } else {
            self.used_bytes += data.len() as u64;
        }

        debug!("used space now {} bytes", self.used_bytes);

        if let Some(e) = error {
            Err(e)
        } else {
            Ok(bucket_path.into_os_string())
        }
    }

    fn free_bucket(&mut self, bucket_path: &OsStr) -> io::Result<u64> {
        unimplemented!();
    }

    fn delete_something(&mut self) -> io::Result<(OsString, u64)> {
        // TODO: this needs a way to tell FSCacheBlockMap to remove its mapping
        unimplemented!();
    }

    fn used_bytes(&self) -> u64 {
        self.used_bytes
    }

    fn max_bytes(&self) -> Option<u64> {
        self.max_bytes
    }
}
