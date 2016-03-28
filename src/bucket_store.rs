// BackFS Filesystem Cache Block Store
//
// Copyright (c) 2016 by William R. Fraser
//

use std::ffi::{OsStr, OsString};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use fsll::PathLinkedList;
use utils;

use libc;
use log;

pub trait CacheBucketStore {
    fn get(&self, bucket_path: &OsStr) -> io::Result<Vec<u8>>;
    fn put(&mut self, data: &[u8]) -> io::Result<OsString>;
    fn delete(&mut self, bucket_path: &OsStr) -> io::Result<u64>;
    fn delete_something(&mut self) -> io::Result<(OsString, u64)>;
    fn used_bytes(&self) -> u64;
}

pub struct FSCacheBucketStore<LL: PathLinkedList> {
    buckets_dir: OsString,
    used_list: LL,
    free_list: LL,
    used_bytes: u64,
    max_bytes: u64,
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
            max_bytes: max_bytes.unwrap_or(0),
            bucket_size: block_size,
            next_bucket_number: 0,
        };
        try!(store.init());
        Ok(store)
    }

    fn init(&mut self) -> io::Result<()> {
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

        if self.max_bytes > 0 && self.used_bytes > self.max_bytes {
            warn!("cache is over-size; freeing buckets until it is within limits");
            while self.used_bytes > self.max_bytes {
                try!(self.delete_something());
            }
        }

        Ok(())
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
}

impl<LL: PathLinkedList> CacheBucketStore for FSCacheBucketStore<LL> {
    fn get(&self, bucket_path: &OsStr) -> io::Result<Vec<u8>> {
        unimplemented!();
    }

    fn put(&mut self, data: &[u8]) -> io::Result<OsString> {
        unimplemented!();
    }

    fn delete(&mut self, bucket_path: &OsStr) -> io::Result<u64> {
        unimplemented!();
    }

    fn delete_something(&mut self) -> io::Result<(OsString, u64)> {
        unimplemented!();
    }

    fn used_bytes(&self) -> u64 {
        self.used_bytes
    }
}
