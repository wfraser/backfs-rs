// BackFS Component Tests
//
// Copyright (c) 2016 by William R. Fraser
//

use std::borrow::Borrow;
use std::ffi::OsStr;
use std::io::{self, Cursor, Write};
use std::str;

extern crate backfs_rs;
use backfs_rs::fscache::*;
//use backfs_rs::block_map::*;
//use backfs_rs::bucket_store::*;

mod test_block_map;
use test_block_map::*;
mod test_bucket_store;
use test_bucket_store::*;
mod sneaky;
use sneaky::*;

macro_rules! stderrln {
    ($($args:tt)+) => { {writeln!(io::stderr(), $($args)+)}.unwrap() };
}

macro_rules! cmp_u8_as_str {
    ($left:expr, $right:expr) => (assert_eq!(str::from_utf8($left).unwrap(), str::from_utf8($right).unwrap()));
}

fn construct_cache(block_size: u64, max_size: Option<u64>)
        -> (FSCache<Sneaky<TestMap>, Sneaky<TestBucketStore>, TestMap, TestBucketStore>,
            Sneaky<TestMap>,
            Sneaky<TestBucketStore>) {
    let mut map_sneak = Sneaky::new(TestMap::new());
    let mut store_sneak = Sneaky::new(TestBucketStore::new(max_size));
    let cache = unsafe { FSCache::<_, _, TestMap, TestBucketStore>::new(map_sneak.sneak(), store_sneak.sneak(), block_size) };
    (cache, map_sneak, store_sneak)
}

#[test]
fn test_fscache_init() {
    let map = TestMap::new();
    let store = TestBucketStore::new(Some(100));
    let mut cache = FSCache::new(map, store, 10);
    assert!(cache.init().is_ok());
}

fn test_fscache_basic(block_size: u64) {
    let data_str = "ABCDEFGHIJKLMN!";
    let mut data = Cursor::new(Vec::from(data_str));
    let filename = OsStr::new("hello.txt");
    let mtime = 1;
    let max_size = Some(100);

    let (mut cache, map_sneak, store_sneak) = construct_cache(block_size, max_size);
    assert!(cache.init().is_ok());

    let map: &TestMap = map_sneak.borrow();
    let store: &TestBucketStore = store_sneak.borrow();

    let fetched: Vec<u8> = cache.fetch(filename, 0, 1024, &mut data, mtime).unwrap();
    assert_eq!(&fetched, data.get_ref());

    let fileblocks = map.map.get(filename).unwrap();
    assert_eq!(fileblocks.mtime, mtime);

    let num_blocks = 1 + ((data_str.len() as u64 - 1) / block_size);
    for i in 0..num_blocks {
        let bucket: u64 = fileblocks.blocks.get(&i).unwrap()
                                           .to_str().unwrap()
                                           .parse().unwrap();
        let cached_data: &Vec<u8> = &store.buckets[bucket as usize]
                                          .data
                                          .as_ref()
                                          .unwrap();

        let end = ::std::cmp::min((i+1) * block_size, data.get_ref().len() as u64) as usize;
        cmp_u8_as_str!(cached_data, &data.get_ref()[(i * block_size) as usize .. end]);
    }

    assert_eq!(fileblocks.blocks.get(&num_blocks), None);
}

#[test]
fn test_fscache_block_sizes() {
    // Check for fencepost errors by doing this with varying block sizes.
    for block_size in 1..31 {
        stderrln!("block size {}", block_size);
        test_fscache_basic(block_size);
    }
}

#[test]
fn test_fscache_out_of_range_read() {
    let data_str = "ABCDEFGHIJKLMN!";
    let mut data = Cursor::new(Vec::from(data_str));
    let filename = OsStr::new("hello.txt");
    let mtime = 1;
    let block_size = 10;
    let max_size = Some(100);

    let (mut cache, map_sneak, store_sneak) = construct_cache(block_size, max_size);
    assert!(cache.init().is_ok());

    let map: &TestMap = map_sneak.borrow();
    let store: &TestBucketStore = store_sneak.borrow();

    // Read 10 bytes at offset 30 (past the end of the file).
    let fetched: Vec<u8> = cache.fetch(filename, 30, 10, &mut data, mtime).unwrap();

    // We should get empty data, but no error.
    assert_eq!(&fetched, &[0u8; 0]);

    // Make sure no blocks were mapped.
    assert!(map.map[filename].blocks.is_empty());

    // Also make sure no buckets got allocated or used.
    assert!(store.buckets.is_empty());
}
