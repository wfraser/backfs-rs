// BackFS Filesystem Cache Block -> Bucket Map
//
// Copyright (c) 2016 by William R. Fraser
//

use std::ffi::{OsStr, OsString};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

pub enum CacheBlockMapResult {
    Bucket(OsString),
    NotPresent,
    Stale,
}

pub trait CacheBlockMap {
    fn get(&self, path: &OsStr, block: u64, mtime: i64) -> io::Result<CacheBlockMapResult>;
    fn put(&mut self, path: &OsStr, block: u64, mtime: i64, bucket_path: &OsStr) -> io::Result<()>;
    fn invalidate<F>(&mut self, path: &OsStr, f: F) -> io::Result<()>
        where F: FnMut(&OsStr) -> io::Result<()>;
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

    fn enumerate_blocks_of_path<F>(&self, map_path: &Path, f: F) -> io::Result<()>
            where F: FnMut(&OsStr) -> io::Result<()> {
        unimplemented!();
    }

}

impl CacheBlockMap  for FSCacheBlockMap {
    fn get(&self, path: &OsStr, block: u64, mtime: i64) -> io::Result<CacheBlockMapResult> {
        unimplemented!();
    }

    fn put(&mut self, path: &OsStr, block: u64, mtime: i64, bucket_path: &OsStr) -> io::Result<()> {
        unimplemented!();
    }

    fn invalidate<F>(&mut self, path: &OsStr, f: F) -> io::Result<()>
            where F: FnMut(&OsStr) -> io::Result<()> {
        let map_path: PathBuf = self.map_path(path);
        try!(self.enumerate_blocks_of_path(&map_path, f));
        if let Err(e) = fs::remove_dir_all(&map_path) {
            error!("Error removing map path {:?}: {}", map_path, e);
            return Err(e);
        }
        Ok(())
    }
}
