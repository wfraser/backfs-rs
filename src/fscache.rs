// BackFS Filesystem Cache
//
// Copyright (c) 2016 by William R. Fraser
//

use std::ffi::{CString, OsStr, OsString};
use std::fmt;
use std::fmt::{Debug, Display};
use std::fs;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem;
use std::os::unix::fs::MetadataExt;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use libc::*;
use walkdir::WalkDir;

use fsll::FSLL;
use link;

pub struct FSCache {
    buckets_dir: OsString,
    map_dir: OsString,
    bucket_list: FSLL,
    free_list: FSLL,
    block_size: u64,
    used_bytes: u64,
    max_bytes: u64,
    next_bucket_number: u64,
    pub debug: bool,
}

macro_rules! log {
    ($s:expr, $fmt:expr) => ($s.log(format_args!(concat!("FSCache: ", $fmt, "\n"))));
    ($s:expr, $fmt:expr, $($arg:tt)*) => ($s.log(format_args!(concat!("FSCache: ", $fmt, "\n"), $($arg)*)));
}

macro_rules! trylog {
    ($s:expr, $e:expr, $fmt:expr) => {
        match $e {
            Ok(x) => x,
            Err(e) => {
                $s.log(format_args!(concat!("FSCache: ", $fmt, ": {}\n"), e));
                return Err(e);
            }
        }
    };
    ($s:expr, $e:expr, $fmt:expr, $($arg:tt)*) => {
        match $e {
            Ok(x) => x,
            Err(e) => {
                $s.log(format_args!(concat!("FSCache: ", $fmt, ": {}\n"), $($arg)*, e));
                return Err(e);
            },
        }
    }
}

impl FSCache {
    pub fn new(cache: &OsString, block_size: u64, max_bytes: u64) -> FSCache {
        let buckets_dir = PathBuf::from(cache).join("buckets").into_os_string();
        FSCache {
            bucket_list: FSLL::new(&buckets_dir, "head", "tail"),
            free_list: FSLL::new(&buckets_dir, "free_head", "free_tail"),
            buckets_dir: buckets_dir,
            map_dir: PathBuf::from(cache).join("map").into_os_string(),
            block_size: block_size,
            used_bytes: 0u64,
            max_bytes: max_bytes,
            next_bucket_number: 0u64,
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

    fn get_fs_size<T: AsRef<Path> + ?Sized + fmt::Debug>(&self, path: &T) -> io::Result<u64> {
        unsafe {
            let path_c = CString::from_vec_unchecked(Vec::from(path.as_ref().as_os_str().as_bytes()));
            let mut statbuf: statvfs = mem::zeroed();
            if -1 == statvfs(path_c.into_raw(), &mut statbuf as *mut statvfs) {
                Err(io::Error::last_os_error())
            } else {
                Ok(statbuf.f_bsize * statbuf.f_blocks)
            }
        }
    }

    pub fn init(&mut self) -> io::Result<()> {
        if self.debug {
            self.bucket_list.debug = true;
            self.free_list.debug = true;
        }

        try!(self.create_dir_and_check_access(&self.buckets_dir));
        try!(self.create_dir_and_check_access(&self.map_dir));

        self.next_bucket_number = try!(self.read_next_bucket_number());
        log!(self, "next bucket number: {}", self.next_bucket_number);

        match self.read_number_file(&PathBuf::from(&self.buckets_dir).join("bucket_size"), Some(self.block_size)) {
            Ok(Some(size)) => {
                if size != self.block_size {
                    let msg = format!(
                        "error: block size in cache ({}) doesn't match the size in the options ({})",
                        size,
                        self.block_size);
                    log!(self, "{}", msg);
                    return Err(io::Error::new(io::ErrorKind::Other, msg));
                }
            },
            Err(e) => {
                let msg = format!("error reading bucket_size file: {}", e);
                log!(self, "{}", msg);
                return Err(io::Error::new(io::ErrorKind::Other, msg));
            },
            Ok(None) => unreachable!()
        }

        self.used_bytes = try!(self.compute_cache_used_size());

        Ok(())
    }

    pub fn used_size(&self) -> u64 {
        self.used_bytes
    }

    pub fn max_size(&self) -> io::Result<u64> {
        if self.max_bytes == 0 {
            self.get_fs_size(&self.buckets_dir)
        } else {
            Ok(self.max_bytes)
        }
    }

    fn compute_cache_used_size(&mut self) -> io::Result<u64> {
        let mut size = 0u64;

        let readdir = trylog!(self, fs::read_dir(Path::new(&self.buckets_dir)),
                              "error getting directory listing for bucket directory");

        for entry_result in readdir {
            let entry = trylog!(self, entry_result, "error reading directory entry");
            let filetype = trylog!(self, entry.file_type(),
                                   "error getting file type of {:?}", entry.file_name());
            if !filetype.is_dir() {
                continue;
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

            let len = match fs::File::open(&path) {
                Ok(file) => {
                    trylog!(self, file.metadata().and_then(|m| Ok(m.len())),
                            "failed to get data file metadata from {:?}", path)
                },
                Err(e) => {
                    if e.raw_os_error() == Some(ENOENT) {
                        0
                    } else {
                        log!(self, "failed to open data file {:?}: {}", path, e);
                        return Err(e);
                    }
                }
            };

            log!(self, "bucket {}: {} bytes", bucket_number, len);

            size += len;
        }

        log!(self, "cache used size: {} bytes", size);

        Ok(size)
    }

    fn map_path<S: AsRef<Path> + ?Sized>(&self, s: &S) -> PathBuf {
        let mut map_path = PathBuf::from(&self.map_dir);

        let path: &Path = s.as_ref();
        let relative_path: &Path = if path.is_absolute() {
            path.strip_prefix("/").unwrap()
        } else {
            path
        };

        map_path.push(relative_path);
        map_path
    }

    fn log(&self, args: fmt::Arguments) {
        if self.debug {
            io::stdout().write_fmt(args).unwrap();
        }
    }

    fn cached_block(&self, path: &OsStr, block: u64) -> Option<Vec<u8>> {
        let bucket_path = match link::getlink(&self.map_path(path), &format!("{}", block)) {
            Ok(Some(path)) => { path },
            Ok(None) => { return None; }
            Err(e) => {
                log!(self, "warning: cached_block error looking up {:?}/{}: {}", path, block, e);
                return None;
            }
        };
        log!(self, "cached_block: bucket path: {:?}", bucket_path);

        if self.bucket_list.to_head(&bucket_path).is_err() {
            // If something's wrong with FSLL, don't read from cache.
            return None;
        }

        let data_path = bucket_path.join("data");
        let mut block_file: File = match File::open(&data_path) {
            Ok(file) => file,
            Err(e) => {
                log!(self, "warning: cached_block error opening bucket data file {:?}: {}", data_path, e);
                return None;
            }
        };

        let mut data: Vec<u8> = Vec::with_capacity(self.block_size as usize);
        match block_file.read_to_end(&mut data) {
            Ok(nread) => {
                log!(self, "cached_block: read {} bytes from cache", nread);
                Some(data)
            },
            Err(e) => {
                log!(self, "warning: cached_block reading from data file {:?}: {}", data_path, e);
                None
            }
        }
    }

    fn open_or_create_file<T: AsRef<Path> + ?Sized + Debug>(&self, path: &T) -> io::Result<(File, bool)> {
        match OpenOptions::new()
                          .read(true)
                          .write(true)
                          .open(path) {
            Ok(file) => Ok((file, false)),
            Err(e) => {
                if e.raw_os_error() == Some(ENOENT) {
                    match OpenOptions::new()
                                      .read(true)
                                      .write(true)
                                      .create(true)
                                      .open(path) {
                        Ok(file) => Ok((file, true)),
                        Err(e) => {
                            log!(self, "error creating file {:?}: {}", path, e);
                            Err(e)
                        }
                    }
                } else {
                    log!(self, "error opening file {:?}: {}", path, e);
                    Err(e)
                }
            }
        }
    }

    fn read_number_file<N: Display + FromStr,
                        T: AsRef<Path> + ?Sized + Debug>(
                            &self,
                            path: &T,
                            default: Option<N>
                        ) -> io::Result<Option<N>>
                        where <N as FromStr>::Err: Debug {
        let (mut file, new) = if default.is_none() {
            // If no default value was given, don't create a file, just open the existing one if
            // there is one, or return None.
            let file = match File::open(path) {
                Ok(file) => file,
                Err(e) => {
                    if e.raw_os_error() == Some(ENOENT) {
                        return Ok(None);
                    } else {
                        return Err(e);
                    }
                }
            };
            (file, false)
        } else {
            try!(self.open_or_create_file(path))
        };

        if new {
            match default {
                Some(n) => match write!(file, "{}", n) {
                    Ok(_) => Ok(Some(n)),
                    Err(e) => {
                        log!(self, "error writing to {:?}: {}", path, e);
                        Err(e)
                    }
                },
                None => Ok(None)
            }
        } else {
            let mut data: Vec<u8> = vec![];
            trylog!(self, file.read_to_end(&mut data),
                "error reading from {:?}", path);

            let string = match String::from_utf8(data) {
                Ok(s) => s,
                Err(e) => {
                    let msg = format!("error interpreting file {:?} as UTF8 string: {}", path, e);
                    log!(self, "{}", msg);
                    return Err(io::Error::new(io::ErrorKind::Other, msg));
                }
            };

            let number: N = match string.trim().parse() {
                Ok(n) => n,
                Err(e) => {
                    let msg = format!("error interpreting file {:?} as number: {:?}", path, e);
                    log!(self, "{}", msg);
                    return Err(io::Error::new(io::ErrorKind::Other, msg));
                }
            };

            Ok(Some(number))
        }
    }

    fn write_number_file<N: Display + FromStr,
                         T: AsRef<Path> + ?Sized + Debug>(
                             &self,
                             path: &T,
                             number: N
                        ) -> io::Result<()> {
        match OpenOptions::new()
                          .write(true)
                          .truncate(true)
                          .create(true)
                          .open(&path) {
            Ok(mut file) => {
                trylog!(self, write!(file, "{}", number),
                    "error writing to {:?}", path);
            },
            Err(e) => {
                log!(self, "error opening {:?}: {}", path, e);
                return Err(e);
            }
        }
        Ok(())
    }

    fn cached_mtime(&self, path: &OsStr) -> Option<i64> {
        let mtime_path: PathBuf = self.map_path(path).join("mtime");

        self.read_number_file(&mtime_path, None::<i64>).unwrap_or_else(|e| {
            log!(self, "warning: error in mtime file {:?}: {}", &mtime_path, e);
            None
        })
    }

    fn write_next_bucket_number(&self, bucket_number: u64) -> io::Result<()> {
        let path = PathBuf::from(&self.buckets_dir).join("next_bucket_number");
        self.write_number_file(&path, bucket_number)
    }

    fn read_next_bucket_number(&self) -> io::Result<u64> {
        let path = PathBuf::from(&self.buckets_dir).join("next_bucket_number");
        self.read_number_file(&path, Some(0u64)).and_then(|r| Ok(r.unwrap()))
    }

    fn write_mtime(&self, path: &OsStr, mtime: i64) -> io::Result<()> {
        let path: PathBuf = self.map_path(path).join("mtime");
        self.write_number_file(&path, mtime)
    }

    fn new_bucket(&mut self) -> io::Result<PathBuf> {
        let bucket_path = PathBuf::from(&self.buckets_dir).join(format!("{}", self.next_bucket_number));
        trylog!(self, fs::create_dir(&bucket_path),
            "error creating bucket directory {:?}", bucket_path);
        self.next_bucket_number += 1;
        trylog!(self, self.write_next_bucket_number(self.next_bucket_number),
            "error writing next bucket number");
        trylog!(self, self.bucket_list.insert_as_head(&bucket_path),
            "error setting bucket as head of used list");
        Ok(bucket_path)
    }

    fn get_bucket(&mut self) -> io::Result<PathBuf> {
        if self.free_list.is_empty() {
            log!(self, "making new bucket");
            self.new_bucket()
        } else {
            let free_bucket: PathBuf = self.free_list.get_tail().unwrap();
            log!(self, "re-using free bucket {:?}", free_bucket);
            try!(self.free_list.disconnect(&free_bucket));
            try!(self.bucket_list.insert_as_head(&free_bucket));
            Ok(free_bucket)
        }
    }

    fn write_block_to_cache(&mut self, path: &OsStr, block: u64, data: &[u8], mtime: i64) {
        let map_path = self.map_path(path);

        let cached_mtime: Option<i64> = self.cached_mtime(path);
        if cached_mtime != Some(mtime) {
            if cached_mtime.is_some() {
                log!(self, "write_block_to_cache: existing mtime is stale; invalidating {:?}", path);
                self.invalidate_path_keep_directories(path);
            } else {
                if let Err(e) = fs::create_dir_all(&map_path) {
                    log!(self, "error creating map directory {:?}: {}", map_path, e);
                    return;
                }
            }

            if let Err(e) = self.write_mtime(path, mtime) {
                log!(self, "error writing mtime file; not writing data to cache: {}", e);
                return;
            }
        }

        while self.used_bytes + (data.len() as u64) > self.max_bytes {
            log!(self, "need to free {} bytes", self.used_bytes + (data.len() as u64) - self.max_bytes);
            if let Err(e) = self.free_last_used_bucket() {
                log!(self, "error freeing up space: {}", e);
                return;
            }
        }

        let bucket_path: PathBuf = match self.get_bucket() {
            Ok(path) => path,
            Err(e) => {
                log!(self, "error getting bucket: {}", e);
                return;
            }
        };
        let data_path = bucket_path.join("data");

        let need_to_free_bucket = match OpenOptions::new()
                                                    .write(true)
                                                    .create(true)
                                                    .open(&data_path) {
            Ok(mut file) => {
                match file.write_all(data) {
                    Ok(()) => {
                        match link::makelink(&map_path, &format!("{}", block),
                                             Some(&bucket_path)) {
                            Ok(()) => {
                                match link::makelink(&bucket_path, "parent",
                                                     Some(&map_path.join(format!("{}", block)))) {
                                    Ok(()) => false,
                                    Err(e) => {
                                        log!(self, "error symlinking bucket to its parent: {}", e);
                                        true
                                    }
                                }
                            }
                            Err(e) => {
                                log!(self, "error symlinking cache bucket into map: {}", e);
                                true
                            }
                        }
                    },
                    Err(e) => {
                        log!(self, "error writing to cache data file: {}", e);
                        true
                    }
                }
            }
            Err(e) => {
                log!(self, "write_block_to_cache: error opening data file {:?}: {}", data_path, e);
                true
            }
        };

        if need_to_free_bucket {
            // Something went wrong; we're not going to use this bucket.
            self.free_bucket(&bucket_path).unwrap();
        } else {
            self.used_bytes += data.len() as u64;
        }
    }

    fn free_last_used_bucket(&mut self) -> io::Result<()> {
        match self.bucket_list.get_tail() {
            Some(last_used_bucket) => {
                try!(self.free_bucket(&last_used_bucket));
            },
            None => {
                log!(self, "free_last_used_bucket: there is no bucket available to free");
            }
        }
        Ok(())
    }

    fn free_bucket<T: AsRef<Path> + ?Sized + Debug>(&mut self, path: &T) -> io::Result<()> {
        log!(self, "freeing bucket {:?}", path);

        trylog!(self, self.bucket_list.disconnect(path),
                "error disconnecting bucket from used list {:?}", path);
        trylog!(self, self.free_list.insert_as_tail(path),
                "error inserting bucket into free list {:?}", path);

        // Remove the parent link if there is one.
        if let Some(parent_path) = trylog!(self, link::getlink(path, "parent"),
                                           "error reading parent link from bucket {:?}", path) {
            log!(self, "removing parent {:?}", &parent_path);
            trylog!(self, link::makelink("", &parent_path, None::<&Path>),
                    "error removing parent link-back {:?}", parent_path);
            self.trim_empty_directories(&parent_path);
        }

        trylog!(self, link::makelink(path, "parent", None::<&Path>),
                "error removing bucket parent link {:?}/parent", path);

        let data_path = PathBuf::from(path.as_ref()).join("data");
        let data_size = {
            let metadata = trylog!(self, fs::metadata(&data_path),
                                   "error getting file metadata of {:?}", &data_path);
            metadata.len()
        };

        trylog!(self, fs::remove_file(&data_path),
                "error removing bucket data file {:?}/data", path);

        log!(self, "freed {} bytes", data_size);
        self.used_bytes -= data_size;
        Ok(())
    }

    pub fn invalidate_path<T: AsRef<Path> + ?Sized + Debug>(&mut self, path: &T) {
        self.invalidate_path_internal(path, true)
    }

    // For use when you're going to turn around and use that path again immediately.
    fn invalidate_path_keep_directories<T: AsRef<Path> + ?Sized + Debug>(&mut self, path: &T) {
        self.invalidate_path_internal(path, false)
    }

    fn invalidate_path_internal<T: AsRef<Path> + ?Sized + Debug>(
            &mut self, path: &T, remove_directories: bool) {
        let map_path: PathBuf = self.map_path(path);
        log!(self, "invalidate_path: {:?}", &map_path);

        for entry_result in WalkDir::new(&map_path) {
            match entry_result {
                Ok(entry) => {
                    let entry_path = entry.path();
                    if entry.file_type().is_symlink() {
                        let bucket_path = match link::getlink("", entry_path) {
                            Ok(Some(path)) => path,
                            Err(e) => {
                                log!(self, "invalidate_path: error reading link {:?}: {}",
                                     entry.path(), e);
                                continue;
                            },
                            Ok(None) => unreachable!()
                        };

                        log!(self, "invalidate_path: invalidating {:?} - freeing {:?}",
                             entry_path, &bucket_path);
                        self.free_bucket(&bucket_path).unwrap();
                        fs::remove_file(entry_path).unwrap();
                    } else if entry.file_type().is_file() && entry.file_name() == "mtime" {
                        fs::remove_file(entry_path).unwrap();
                    }
                },
                Err(e) => {
                    let is_start = e.path() == Some(&map_path);
                    let ioerr = io::Error::from(e);
                    if is_start && ioerr.raw_os_error() == Some(ENOENT) {
                        // If the map directory doesn't exist, there's nothing to do.
                        return;
                    } else {
                        log!(self, "invalidate_path: error reading directory entry from {:?}: {:?}",
                             map_path, ioerr);
                    }
                }
            }
        }

        if remove_directories {
            // Now we've freed all buckets under map_path; remove the map directories under here.
            fs::remove_dir_all(&map_path).unwrap();
            self.trim_empty_directories(&map_path);
        }
    }

    fn trim_empty_directories<T: AsRef<Path> + ?Sized + Debug>(&self, path: &T) {
        // Walk back up the tree and remove any map directories that are now empty.
        let mut parent_path: &Path = path.as_ref();
        loop {
            match parent_path.parent() {
                Some(path) => { parent_path = path; },
                None => { break; }
            }

            if parent_path == Path::new(&self.map_dir) {
                break;
            }

            if let Err(e) = fs::remove_dir(parent_path) {
                if e.raw_os_error() != Some(ENOTEMPTY) {
                    log!(self, "invalidate_path: failed to remove empty map directory {:?}: {}",
                         parent_path, e);
                }
                break;
            } else {
                log!(self, "invalidate_path: removed empty map directory {:?}", parent_path);
            }
        }
    }

    fn is_bucket_orphaned<T: AsRef<Path> + ?Sized + Debug>(&self, path: &T) -> bool {
        let parent = match link::getlink(path, "parent") {
            Ok(Some(path)) => path,
            Ok(None) => { return false; },
            Err(e) => {
                log!(self, "is_bucket_orphaned: error reading parent link of {:?}: {}", path, e);
                return false;
            }
        };

        let parent_link_back = match link::getlink("", &parent) {
            Ok(Some(path)) => path,
            Ok(None) => {
                log!(self, "bucket is orphaned - no link back from parent: {:?} -> {:?}",
                     path, parent);
                return true;
            },
            Err(e) => {
                log!(self, "is_bucket_orphaned: error reading link back from {:?}: {}", parent, e);
                return true;
            },
        };

        if parent_link_back != path.as_ref() {
            log!(self, "bucket is orphaned - parent links elsewhere: {:?} -> {:?} -> {:?}",
                 path, parent, parent_link_back);
            true
        } else {
            false
        }
    }

    pub fn free_orphaned_buckets(&mut self) {
        log!(self, "free_orphaned_buckets");

        let entries = match fs::read_dir(&self.buckets_dir) {
            Ok(entries) => entries,
            Err(e) => {
                log!(self, "free_orphaned_buckets: error opening buckets directory: {}", e);
                return;
            }
        };

        for entry_result in entries {
            match entry_result {
                Ok(entry) => {
                    if entry.file_type().unwrap().is_dir() {
                        let entry_path = entry.path();
                        if self.is_bucket_orphaned(&entry_path) {
                            if let Err(e) = self.free_bucket(&entry_path) {
                                log!(self, "free_orphaned_buckets: error freeing {:?}: {}",
                                     entry_path, e);
                            }
                        }
                    }
                },
                Err(e) => {
                    log!(self, "free_orphaned_buckets: error listing buckets directory: {}", e);
                    return;
                }
            }
        }
    }

    pub fn fetch(&mut self, path: &OsStr, offset: u64, size: u64, file: &mut fs::File) -> io::Result<Vec<u8>> {
        let mtime = try!(file.metadata()).mtime() as i64;

        let cached_mtime = self.cached_mtime(path);
        if cached_mtime != Some(mtime) {
            if cached_mtime.is_some() {
                log!(self, "cached data is stale, invalidating: {:?}", path);
            }
            self.invalidate_path_keep_directories(path);
        }

        let first_block = offset / self.block_size;
        let last_block  = (offset + size - 1) / self.block_size;

        log!(self, "fetching blocks {} to {} from {:?}", first_block, last_block, path);

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

                    // TODO: try to write into a slice of `result` in place instead of writing to
                    // a new buffer and moving the data later.

                    let mut buf: Vec<u8> = Vec::with_capacity(self.block_size as usize);
                    unsafe {
                        buf.set_len(self.block_size as usize);
                    }

                    let nread = try!(file.read(&mut buf[..])) as u64;
                    log!(self, "read {} bytes", nread);

                    if nread != self.block_size {
                        buf.truncate(nread as usize);
                    }

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

            if nread < block_size {
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
