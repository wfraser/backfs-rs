// BackFS Filesystem Cache Block Store
//
// Copyright (c) 2016 by William R. Fraser
//

use std::ffi::{OsStr, OsString};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use fsll::PathLinkedList;
use link;
use utils;

use libc;
use log;

pub trait CacheBucketStore {
    fn init<F>(&mut self, mut delete_handler: F) -> io::Result<()>
        where F: FnMut(/* deleted bucket parent path */ &OsStr) -> io::Result<()>;
    fn get(&self, bucket_path: &OsStr) -> io::Result<Vec<u8>>;
    fn put<F>(&mut self, parent: &OsStr, data: &[u8], mut delete_handler: F) -> io::Result<OsString>
        where F: FnMut(/* deleted bucket parent path */ &OsStr) -> io::Result<()>;
    fn free_bucket(&mut self, bucket_path: &OsStr) -> io::Result<u64>;
    fn delete_something(&mut self) -> io::Result<(OsString, u64)>;
    fn used_bytes(&self) -> u64;
    fn max_bytes(&self) -> Option<u64>;
    fn enumerate_buckets<F>(&self, mut handler: F) -> io::Result<()>
        where F: FnMut(/* bucket path */ &OsStr,
                       /* parent path */ Option<&OsStr>) -> io::Result<()>;
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
            -> FSCacheBucketStore<LL> {
        FSCacheBucketStore {
            buckets_dir: buckets_dir,
            used_list: used_list,
            free_list: free_list,
            used_bytes: 0,
            max_bytes: max_bytes,
            bucket_size: block_size,
            next_bucket_number: 0,
        }
    }

    fn read_next_bucket_number(&self) -> io::Result<u64> {
        let path = PathBuf::from(&self.buckets_dir).join("next_bucket_number");
        utils::read_number_file(&path, Some(0u64)).and_then(|r| Ok(r.unwrap()))
    }

    fn write_next_bucket_number(&self, bucket_number: u64) -> io::Result<()> {
        let path = PathBuf::from(&self.buckets_dir).join("next_bucket_number");
        utils::write_number_file(&path, bucket_number)
    }

    fn for_each_bucket<F>(&self, mut handler: F) -> io::Result<()>
            where F: FnMut(&OsStr) -> io::Result<()> {
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

            if let Err(e) = handler(path.as_os_str()) {
                error!("for_each_bucket: handler returned {}", e);
                return Err(e);
            }
        }
        Ok(())
    }

    fn compute_cache_used_size(&mut self) -> io::Result<u64> {
        let mut size = 0u64;

        try!(self.for_each_bucket(|bucket_path| {
            let path = PathBuf::from(bucket_path).join("data");

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
            Ok(())
        }));

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
    fn init<F>(&mut self, mut delete_handler: F) -> io::Result<()>
            where F: FnMut(&OsStr) -> io::Result<()> {
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
                let (map_path, _) = try!(self.delete_something());
                trylog!(delete_handler(&map_path),
                        "delete handler returned error");
            }
        }

        Ok(())
    }

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

    fn put<F>(&mut self, parent: &OsStr, data: &[u8], mut delete_handler: F) -> io::Result<OsString>
            where F: FnMut(&OsStr) -> io::Result<()> {
        loop {
            let bytes_needed = self.free_bytes_needed_for_write(data.len() as u64);
            if bytes_needed > 0 {
                info!("put: need to free {} bytes", bytes_needed);
                let (map_path, _) = trylog!(self.delete_something(),
                                               "put: error freeing up space");
                trylog!(delete_handler(&map_path),
                        "put: delete handler returned error");
            } else {
                break;
            }
        }

        let bucket_path: PathBuf = trylog!(self.get_bucket(),
                                           "put: error getting bucket");

        trylog!(link::makelink(&bucket_path, "parent", Some(parent)),
                "failed to write parent link from bucket {:?} to {:?}", bucket_path, parent);

        let data_path = bucket_path.join("data");

        let mut data_file = trylog!(OpenOptions::new()
                                                .write(true)
                                                .create(true)
                                                .open(&data_path),
                                    "put: error opening data file {:?}", data_path);

        loop {
            match data_file.write_all(data) {
                Ok(()) => { break; },
                Err(e) => {
                    if e.raw_os_error() == Some(libc::ENOSPC) {
                        info!("put: out of space; freeing buckets");
                        let (map_path, n) = trylog!(self.delete_something(),
                                                        "put: error freeing up space");
                        trylog!(delete_handler(&map_path),
                                "put: delete handler returned error");
                        info!("freed {} bytes; trying the write again", n);
                    } else {
                        error!("put: error writing to cache data file {:?}: {}", data_path, e);
                        return Err(e);
                    }
                }
            }
        }

        self.used_bytes += data.len() as u64;
        debug!("used space now {} bytes", self.used_bytes);

        Ok(bucket_path.into_os_string())
    }

    fn free_bucket(&mut self, bucket_path: &OsStr) -> io::Result<u64> {
        debug!("freeing bucket {:?}", bucket_path);

        trylog!(self.used_list.disconnect(bucket_path),
                "error disconnecting bucket from used list {:?}", bucket_path);
        trylog!(self.free_list.insert_as_tail(bucket_path),
                "error inserting bucket into free list {:?}", bucket_path);

        let data_path = PathBuf::from(bucket_path).join("data");
        let data_size: u64 = match fs::metadata(&data_path) {
            Ok(metadata) => {
                trylog!(fs::remove_file(&data_path),
                        "error removing bucket data file {:?}", &data_path);
                metadata.len() as u64
            },
            Err(e) => {
                debug!("error getting data file metadata of {:?}: {}", &data_path, e);
                0
            }
        };

        info!("freed {} bytes", data_size);
        self.used_bytes -= data_size;
        Ok(data_size)
    }

    fn delete_something(&mut self) -> io::Result<(OsString, u64)> {
        let bucket_path: PathBuf = match self.used_list.get_tail() {
            Some(path) => path,
            None => {
                error!("can't free anything; the used list is empty!");
                return Err(io::Error::from_raw_os_error(libc::EINVAL));
            },
        };
        let parent: PathBuf = match link::getlink(&bucket_path, "parent") {
            Ok(Some(path)) => path,
            Ok(None) => {
                error!("delete_something: bucket {:?} has no parent", bucket_path);
                return Err(io::Error::from_raw_os_error(libc::EINVAL));
            },
            Err(e) => {
                error!("delete_something: error reading parent link for {:?}: {}",
                       bucket_path, e);
                return Err(e);
            }
        };
        let bytes_freed = trylog!(self.free_bucket(bucket_path.as_os_str()),
                                  "error freeing bucket {:?}", bucket_path);
        Ok((parent.into_os_string(), bytes_freed))
    }

    fn used_bytes(&self) -> u64 {
        self.used_bytes
    }

    fn max_bytes(&self) -> Option<u64> {
        self.max_bytes
    }

    fn enumerate_buckets<F>(&self, mut handler: F) -> io::Result<()>
            where F: FnMut(&OsStr, Option<&OsStr>) -> io::Result<()> {

        try!(self.for_each_bucket(|bucket_path| {
            let parent_opt = trylog!(link::getlink(bucket_path, "parent"),
                                     "Failed to read parent link for {:?}", bucket_path);
            let parent_osstr_opt = parent_opt.as_ref().map(|x| x.as_ref());
            trylog!(handler(bucket_path, parent_osstr_opt),
                    "enumerate_buckets: handler returned");
            Ok(())
        }));

        Ok(())
    }
}
