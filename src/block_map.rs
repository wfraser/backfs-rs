// BackFS Filesystem Cache Block -> Bucket Map
//
// Copyright (c) 2016 by William R. Fraser
//

use std::ffi::{OsStr, OsString};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use link;
use utils;

use libc;
use log;
use walkdir::WalkDir;

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

#[must_use]
#[derive(PartialEq)]
pub enum CacheBlockMapFileResult {
    Current,
    Stale,
    NotPresent,
}

pub trait CacheBlockMap {
    fn check_file_mtime(&self, path: &OsStr, mtime: i64) -> io::Result<CacheBlockMapFileResult>;
    fn set_file_mtime(&mut self, path: &OsStr, mtime: i64) -> io::Result<()>;
    fn get_block(&self, path: &OsStr, block: u64) -> io::Result<Option<OsString>>;
    fn put_block(&mut self, path: &OsStr, block: u64, bucket_path: &OsStr) -> io::Result<()>;
    fn get_block_path(&self, path: &OsStr, block: u64) -> OsString;
    fn invalidate_path<F>(&mut self, path: &OsStr, delete_handler: F) -> io::Result<()>
        where F: FnMut(&OsStr) -> io::Result<()>;
    fn unmap_block(&mut self, block_path: &OsStr) -> io::Result<()>;
}

pub struct FSCacheBlockMap {
    map_dir: OsString,
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

    fn enumerate_blocks_of_path<F>(&self, map_path: &Path, mut f: F) -> io::Result<()>
            where F: FnMut(&OsStr) -> io::Result<()> {
        for entry_result in WalkDir::new(map_path) {
            match entry_result {
                Ok(entry) => {
                    let entry_path = entry.path();
                    if entry.file_type().is_symlink() {
                        let bucket_path = match link::getlink("", entry_path) {
                            Ok(Some(path)) => path,
                            Err(e) => {
                                error!("enumerate_blocks_of_path: error reading link {:?}: {}",
                                     entry.path(), e);
                                continue;
                            },
                            Ok(None) => unreachable!()
                        };

                        trylog!(f(bucket_path.as_os_str()),
                                "enumerate_blocks_of_path: callback returned error");
                    }
                },
                Err(e) => {
                    let is_start = e.path() == Some(map_path);
                    let ioerr = io::Error::from(e);
                    if is_start && ioerr.raw_os_error() == Some(libc::ENOENT) {
                        // If the map directory doesn't exist, there's nothing to do.
                        return Ok(())
                    } else {
                        error!("enumerate_blocks_of_path: error reading directory entry from {:?}: {}",
                               map_path, ioerr);
                        return Err(ioerr)
                    }
                }
            }
        }
        Ok(())
    }

}

impl CacheBlockMap for FSCacheBlockMap {
    fn check_file_mtime(&self, path: &OsStr, mtime: i64) -> io::Result<CacheBlockMapFileResult> {
        let mtime_file = self.map_path(path).join("mtime");
        match utils::read_number_file(&mtime_file, None::<i64>) {
            Ok(Some(n)) => {
                if n == mtime {
                    Ok(CacheBlockMapFileResult::Current)
                } else {
                    Ok(CacheBlockMapFileResult::Stale)
                }
            },
            Ok(None) => Ok(CacheBlockMapFileResult::NotPresent),
            Err(e) => {
                error!("problem with mtime file {:?}: {}", &mtime_file, e);
                Err(e)
            }
        }
    }

    fn set_file_mtime(&mut self, path: &OsStr, mtime: i64) -> io::Result<()> {
        let file_map_dir = self.map_path(path);
        trylog!(fs::create_dir_all(&file_map_dir),
                "set_file_mtime: error creating {:?}", file_map_dir);

        let mtime_file = file_map_dir.join("mtime");
        trylog!(utils::write_number_file(&mtime_file, mtime),
                "failed to write mtime file {:?}", mtime_file);

        Ok(())
    }

    fn get_block(&self, path: &OsStr, block: u64) -> io::Result<Option<OsString>> {
        let file_map_dir = self.map_path(path);
        match link::getlink(&file_map_dir, &format!("{}", block)) {
            Ok(Some(pathbuf)) => Ok(Some(pathbuf.into_os_string())),
            Ok(None) => Ok(None),
            Err(e) => Err(e)
        }
    }

    fn put_block(&mut self, path: &OsStr, block: u64, bucket_path: &OsStr) -> io::Result<()> {
        debug!("mapping {:?}/{} to {:?}", path, block, bucket_path);
        let file_block = self.map_path(path).join(format!("{}", block));
        trylog!(link::makelink("", &file_block, Some(bucket_path)),
                "error making map link from {:?} to {:?}", &file_block, bucket_path);
        debug_assert_eq!(link::getlink(bucket_path, "parent").unwrap(), Some(file_block));
        Ok(())
    }

    fn get_block_path(&self, path: &OsStr, block: u64) -> OsString {
        self.map_path(path).join(format!("{}", block)).into_os_string()
    }

    fn invalidate_path<F>(&mut self, path: &OsStr, f: F) -> io::Result<()>
            where F: FnMut(&OsStr) -> io::Result<()> {
        let map_path: PathBuf = self.map_path(path);
        try!(self.enumerate_blocks_of_path(&map_path, f));
        trylog!(fs::remove_dir_all(&map_path),
                "Error removing map path {:?}", map_path);
        Ok(())
    }

    fn unmap_block(&mut self, map_block_path: &OsStr) -> io::Result<()> {
        debug!("unmapping {:?}", &map_block_path);

        trylog!(fs::remove_file(&map_block_path),
                "unable to remove map block link {:?}", map_block_path);

        // TODO: clean up parents

        Ok(())
    }
}
