// BackFS Filesystem Tests :: Cache Block -> Bucket Map
//
// Copyright (c) 2016 by William R. Fraser
//

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::io;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::str;

extern crate backfs_rs;
use backfs_rs::block_map::*;

pub struct TestMapData {
    pub mtime: i64,
    pub blocks: BTreeMap<u64, OsString>,
}

pub struct TestMap {
    pub map: BTreeMap<OsString, TestMapData>,
}

impl TestMap {
    pub fn new() -> TestMap {
        TestMap {
            map: BTreeMap::new()
        }
    }
}

impl CacheBlockMap for TestMap {
    fn check_file_mtime(&self, path: &OsStr, mtime: i64) -> io::Result<CacheBlockMapFileResult> {
        match self.map.get(path) {
            Some(entry) => {
                if entry.mtime == mtime {
                    Ok(CacheBlockMapFileResult::Current)
                } else {
                    Ok(CacheBlockMapFileResult::Stale)
                }
            },
            None => Ok(CacheBlockMapFileResult::NotPresent)
        }
    }

    fn set_file_mtime(&mut self, path: &OsStr, mtime: i64) -> io::Result<()> {
        self.map.entry(path.to_os_string())
                           .or_insert(TestMapData{
                               mtime: mtime,
                               blocks: BTreeMap::new(),
                           });
        Ok(())
    }

    fn get_block(&self, path: &OsStr, block: u64) -> io::Result<Option<OsString>> {
        match self.map.get(path) {
            Some(entry) => {
                match entry.blocks.get(&block) {
                    Some(bucket_path) => Ok(Some(bucket_path.clone())),
                    None => Ok(None)
                }
            }
            // Checking the file mtime is what creates the entry.
            None => { panic!("you can't check for blocks before checking the file mtime!"); }
        }
    }

    fn put_block(&mut self, path: &OsStr, block: u64, bucket_path: &OsStr) -> io::Result<()> {
        match self.map.get_mut(path) {
            Some(entry) => {
                entry.blocks.insert(block, bucket_path.to_os_string());
                Ok(())
            },
            // Checking the file mtime is what creates the entry.
            None => { panic!("you can't add blocks before checking the file mtime!"); }
        }
    }

    fn get_block_path(&self, path: &OsStr, block: u64) -> OsString {
        let mut bytes = Vec::from(path.as_bytes());
        bytes.append(&mut format!("/{}", block).into_bytes());
        OsString::from_vec(bytes)
    }

    fn invalidate_path<F>(&mut self, _path: &OsStr, _f: F) -> io::Result<()>
            where F: FnMut(&OsStr) -> io::Result<()> {
        // TODO
        unimplemented!();
    }

    /*
    fn unmap_bucket(&mut self, bucket_path: &OsStr) -> io::Result<()> {
        self.map.remove(bucket_path);
        Ok(())
    }
    */

    fn unmap_block(&mut self, block_path: &OsStr) -> io::Result<()> {
        let parts: Vec<&[u8]> = block_path.as_bytes().rsplitn(2, |byte| *byte == b'/').collect();
        let path = OsStr::from_bytes(parts[0]);
        let block: u64 = str::from_utf8(&parts[0][1..]).unwrap().parse().unwrap();
        let file = self.map.get_mut(path).unwrap();
        file.blocks.remove(&block);
        Ok(())
    }

    fn is_block_mapped(&self, block_path: &OsStr) -> io::Result<bool> {
        let parts: Vec<&[u8]> = block_path.as_bytes().rsplitn(2, |byte| *byte == b'/').collect();
        let path = OsStr::from_bytes(parts[1]);
        let block: u64 = str::from_utf8(&parts[0]).unwrap().parse().unwrap();
        Ok(match self.map.get(path) {
            Some(file_entry) => file_entry.blocks.contains_key(&block),
            None => false
        })
    }
}
