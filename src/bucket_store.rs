// BackFS Filesystem Cache Block Store
//
// Copyright 2016-2021 by William R. Fraser
//

use std::ffi::{OsStr, OsString};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::fsll::PathLinkedList;
use crate::link;
use crate::utils;

pub trait CacheBucketStore {
    fn init<F>(&mut self, delete_handler: F) -> io::Result<()>
        where F: FnMut(/* deleted bucket parent path */ &OsStr) -> io::Result<()>;
    fn get(&self, bucket_path: &OsStr) -> io::Result<Vec<u8>>;
    fn put<F>(&mut self, parent: &OsStr, data: &[u8], delete_handler: F) -> io::Result<OsString>
        where F: FnMut(/* deleted bucket parent path */ &OsStr) -> io::Result<()>;
    fn free_bucket(&mut self, bucket_path: &OsStr) -> io::Result<u64>;
    fn delete_something(&mut self) -> io::Result<(OsString, u64)>;
    fn used_bytes(&self) -> u64;
    fn max_bytes(&self) -> Option<u64>;
    fn enumerate_buckets<F>(&self, handler: F) -> io::Result<()>
        where F: FnMut(/* bucket path */ &OsStr,
                       /* parent path */ Option<&OsStr>) -> io::Result<()>;
    fn get_size(&self, bucket_path: &OsStr) -> io::Result<u64>;
}

pub struct FsCacheBucketStore<LL: PathLinkedList> {
    buckets_dir: OsString,
    used_list: LL,
    free_list: LL,
    used_bytes: u64,
    max_bytes: Option<u64>,
    bucket_size: u64,
    next_bucket_number: u64,
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

impl<LL: PathLinkedList> FsCacheBucketStore<LL> {
    pub fn new(buckets_dir: OsString, used_list: LL, free_list: LL, block_size: u64, max_bytes: Option<u64>)
            -> Self {
        Self {
            buckets_dir,
            used_list,
            free_list,
            used_bytes: 0,
            max_bytes,
            bucket_size: block_size,
            next_bucket_number: 0,
        }
    }

    fn read_next_bucket_number(&self) -> io::Result<u64> {
        let path = PathBuf::from(&self.buckets_dir).join("next_bucket_number");
        utils::read_number_file(&path, Some(0u64)).map(|r| r.unwrap())
    }

    fn write_next_bucket_number(&self, bucket_number: u64) -> io::Result<()> {
        let path = PathBuf::from(&self.buckets_dir).join("next_bucket_number");
        utils::write_number_file(path, &bucket_number)
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

            match entry.file_name().to_str().map(|name| name.parse::<u64>()) {
                Some(Err(_)) | None => {
                    // folder name doesn't parse as a number: must not be a bucket. Skip it.
                    continue;
                },
                Some(Ok(_)) => (),
            }

            let mut path: OsString = self.buckets_dir.clone();
            path.push(OsString::from("/"));
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

        self.for_each_bucket(|bucket_path| {
            let path = PathBuf::from(bucket_path).join("data");

            let len = match fs::File::open(&path) {
                Ok(file) => {
                    trylog!(file.metadata().map(|m| m.len()),
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
        })?;

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
            self.free_list.disconnect(&free_bucket)?;
            self.used_list.insert_as_head(&free_bucket)?;
            Ok(free_bucket)
        }
    }

    fn new_bucket(&mut self) -> io::Result<PathBuf> {
        let bucket_path = PathBuf::from(&self.buckets_dir).join(format!("{}", self.next_bucket_number));
        // TODO: this should be a retry loop
        trylog!(fs::create_dir(&bucket_path),
                "error creating bucket directory {:?}", bucket_path);
        trylog!(self.write_next_bucket_number(self.next_bucket_number + 1),
                "error writing next bucket number");
        trylog!(self.used_list.insert_as_head(&bucket_path),
                "error setting bucket as head of used list");
        self.next_bucket_number += 1;
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

impl<LL: PathLinkedList> CacheBucketStore for FsCacheBucketStore<LL> {
    fn init<F>(&mut self, mut delete_handler: F) -> io::Result<()>
            where F: FnMut(&OsStr) -> io::Result<()> {
        self.next_bucket_number = self.read_next_bucket_number()?;
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
                    return Err(io::Error::other(msg));
                }
            },
            Err(e) => {
                let msg = format!("error reading bucket_size file: {}", e);
                error!("{}", msg);
                return Err(io::Error::other(msg));
            },
            Ok(None) => unreachable!()
        }

        self.used_bytes = self.compute_cache_used_size()?;

        if self.max_bytes.is_some() && self.used_bytes > self.max_bytes.unwrap() {
            warn!("cache is over-size; freeing buckets until it is within limits");
            while self.used_bytes > self.max_bytes.unwrap() {
                let (map_path, _) = self.delete_something()?;
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
            "cached_block error opening bucket data file {:?}", data_path);

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

    #[allow(clippy::cognitive_complexity)] // the retry loops really blow this up
    fn put<F>(&mut self, parent: &OsStr, data: &[u8], mut delete_handler: F) -> io::Result<OsString>
            where F: FnMut(&OsStr) -> io::Result<()>
    {
        macro_rules! innerlog {
            ($level:expr, $e:expr, $fmt:expr, $($args:tt)+) => {
                log!($level, concat!($fmt, ": {}"), $($args)+, $e);
            };
            ($level:expr, $e:expr, $fmt:expr) => {
                log!($level, concat!($fmt, ": {}"), $e);
            }
        }

        macro_rules! retry_enospc {
            ($e:expr, $($errlog:tt)*) => {
                {
                    let retval;
                    loop {
                        match $e {
                            Ok(x) => {
                                retval = x;
                                break;
                            },
                            Err(ref e) if e.raw_os_error() == Some(libc::ENOSPC) => {
                                innerlog!(log::Level::Info, e, $($errlog)*);
                                let (map_path, n) = trylog!(self.delete_something(),
                                                            "put: error freeing up space");
                                trylog!(delete_handler(&map_path),
                                        "put: delete handler returned error");
                                info!("freed {} bytes; trying again", n);
                            },
                            Err(e) => {
                                innerlog!(log::Level::Error, e, $($errlog)*);
                                return Err(e);
                            }
                        }
                    }
                    retval
                }
            }
        }

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

        let bucket_path = retry_enospc!(self.get_bucket(), "put: error getting bucket");
        retry_enospc!(link::makelink(&bucket_path, "parent", Some(parent)),
                      "put: failed to write parent link from bucket {:?} to {:?}",
                      bucket_path, parent);

        let data_path = bucket_path.join("data");
        let mut data_file = retry_enospc!(
            OpenOptions::new()
                        .write(true)
                        .create(true)
                        .truncate(true)
                        .open(&data_path),
            "put: error opening data file {:?}", data_path
        );

        retry_enospc!(data_file.seek(SeekFrom::Start(0)).and_then(|_| data_file.write_all(data)),
                      "put: failed to write to cache data file {:?}", data_path);

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
                metadata.len()
            },
            Err(e) => {
                debug!("error getting data file metadata of {:?}: {}", &data_path, e);
                0
            }
        };

        let parent_link = PathBuf::from(bucket_path).join("parent");
        trylog!(fs::remove_file(&parent_link),
                "unable to remove block parent link {:?}", parent_link);

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

        self.for_each_bucket(|bucket_path| {
            let parent_opt: Option<PathBuf> = trylog!(link::getlink(bucket_path, "parent"),
                    "Failed to read parent link for {:?}", bucket_path);
            let parent_osstr_opt: Option<&OsStr> = parent_opt.as_ref().map(AsRef::as_ref);
            trylog!(handler(bucket_path, parent_osstr_opt), "enumerate_buckets: handler returned");
            Ok(())
        })?;

        Ok(())
    }

    fn get_size(&self, bucket_path: &OsStr) -> io::Result<u64> {
        let data_path = PathBuf::from(bucket_path).join("data");
        let metadata = fs::metadata(data_path)?;
        Ok(metadata.len())
    }
}
