// BackFS Filesystem Cache Block -> Bucket Map
//
// Copyright (c) 2016 by William R. Fraser
//

use std::boxed::Box;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use link;
use utils;

use log;

macro_rules! log2 {
    ($lvl:expr, $($arg:tt)+) => (
        log!(target: "BlockMap", $lvl, $($arg)+));
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

pub enum CacheBlockMapFileEntryResult {
    Entry(Box<CacheBlockMapFileEntry>),
    StaleDataPresent,
}

pub trait CacheBlockMapFileEntry {
    fn get(&self, block: u64) -> io::Result<Option<OsString>>;
    fn put(&self, block: u64, bucket_path: &OsStr) -> io::Result<()>;
}

pub trait CacheBlockMap {
    fn get_file_entry(&mut self, path: &OsStr, mtime: i64)
        -> io::Result<CacheBlockMapFileEntryResult>;
    fn invalidate_path<F>(&mut self, path: &OsStr, f: F) -> io::Result<()>
        where F: FnMut(&OsStr) -> io::Result<()>;
    fn unmap_bucket(&mut self, bucket_path: &OsStr) -> io::Result<()>;
}

pub struct FSCacheBlockMap {
    map_dir: OsString,
}

pub struct FSCacheBlockMapFileEntry {
    file_map_dir: OsString,
}

impl FSCacheBlockMap {
    pub fn new(map_dir: OsString) -> FSCacheBlockMap {
        FSCacheBlockMap {
            map_dir: map_dir,
        }
    }

    fn map_path(&self, path: &OsStr) -> PathBuf {
        let mut map_path = PathBuf::from(&self.map_dir);

        let path: &Path = Path::new(path);
        let relative_path: &Path = if path.is_absolute() {
            path.strip_prefix("/").unwrap()
        } else {
            path
        };

        map_path.push(relative_path);
        map_path
    }

    fn enumerate_blocks_of_path<F>(&self, map_path: &Path, f: F) -> io::Result<()>
            where F: FnMut(&OsStr) -> io::Result<()> {
        unimplemented!();
    }

}

impl CacheBlockMap for FSCacheBlockMap {
    fn get_file_entry(&mut self, path: &OsStr, mtime: i64)
            -> io::Result<CacheBlockMapFileEntryResult> {
        let map_path = self.map_path(path);
        debug!("get_file_entry: {:?}", map_path);

        trylog!(fs::create_dir_all(&map_path),
                "get_file_entry: error creating {:?}", map_path);

        let mtime_path = map_path.join("mtime");
        match utils::read_number_file(&mtime_path, None::<i64>) {
            Ok(Some(n)) => {
                if n != mtime {
                    info!("cached data is stale: {:?}", path);
                    return Ok(CacheBlockMapFileEntryResult::StaleDataPresent);
                }
            },
            Ok(None) => {
                try!(utils::write_number_file(&mtime_path, mtime));
            },
            Err(e) => {
                error!("problem with mtime file {:?}: {}", &mtime_path, e);
                return Err(e);
            }
        }

        Ok(CacheBlockMapFileEntryResult::Entry(Box::new(FSCacheBlockMapFileEntry {
            file_map_dir: map_path.into_os_string()
        })))
    }

    fn invalidate_path<F>(&mut self, path: &OsStr, f: F) -> io::Result<()>
            where F: FnMut(&OsStr) -> io::Result<()> {
        let map_path: PathBuf = self.map_path(path);
        try!(self.enumerate_blocks_of_path(&map_path, f));
        trylog!(fs::remove_dir_all(&map_path),
                "Error removing map path {:?}", map_path);
        Ok(())
    }

    fn unmap_bucket(&mut self, bucket_path: &OsStr) -> io::Result<()> {
        let map_block_path = match link::getlink(bucket_path, "parent") {
            Ok(Some(path)) => path,
            Ok(None) => {
                // We have no idea where this bucket is mapped...
                warn!("trying to unmap a bucket that lacks a parent link: {:?}", bucket_path);
                return Ok(());
            }
            Err(e) => {
                error!("unable to read parent link from bucket {:?}: {}", bucket_path, e);
                return Err(e);
            }
        };

        trylog!(fs::remove_file(&map_block_path),
                "unable to remove map block link {:?}", map_block_path);

        trylog!(link::makelink(bucket_path, "parent", None::<&Path>),
                "unable to remove parent link in {:?}", bucket_path);

        // TODO: clean up parents

        Ok(())
    }
}

impl CacheBlockMapFileEntry for FSCacheBlockMapFileEntry {
    fn get(&self, block: u64) -> io::Result<Option<OsString>> {
        match link::getlink(&self.file_map_dir, &format!("{}", block)) {
            Ok(Some(pathbuf)) => Ok(Some(pathbuf.into_os_string())),
            Ok(None) => Ok(None),
            Err(e) => Err(e)
        }
    }
    fn put(&self, block: u64, bucket_path: &OsStr) -> io::Result<()> {
        trylog!(link::makelink(&self.file_map_dir, &format!("{}", block), Some(bucket_path)),
                "error making map link from {:?}/{} to {:?}", &self.file_map_dir, block, bucket_path);
        Ok(())
    }
}
