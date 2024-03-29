// BackFS Component Tests
//
// Copyright 2016-2021 by William R. Fraser
//

#![deny(rust_2018_idioms)]

use std::borrow::{Borrow, BorrowMut};
use std::ffi::OsStr;
use std::io::Cursor;
use std::str;

use backfs::fscache::*;
use backfs::block_map::*;
use backfs::bucket_store::*;

mod mocks;
use mocks::test_block_map::*;
use mocks::test_bucket_store::*;
use mocks::sneaky::*;

macro_rules! cmp_u8_as_str {
    ($left:expr, $right:expr) => (assert_eq!(
            str::from_utf8($left).unwrap(),
            str::from_utf8($right).unwrap()));
}

#[allow(clippy::type_complexity)]
fn construct_cache(block_size: u64, max_size: Option<u64>)
        -> (FsCache<Sneaky<TestMap>, TestMap, Sneaky<TestBucketStore>, TestBucketStore>,
            Sneaky<TestMap>,
            Sneaky<TestBucketStore>) {
    let mut map_sneak = Sneaky::new(TestMap::default());
    let mut store_sneak = Sneaky::new(TestBucketStore::new(max_size));
    let cache = unsafe {
        FsCache::<_, TestMap, _, TestBucketStore>::new(
            map_sneak.sneak(),
            store_sneak.sneak(),
            block_size)
    };
    (cache, map_sneak, store_sneak)
}

#[test]
fn test_fscache_init() {
    let map = TestMap::default();
    let store = TestBucketStore::new(Some(100));
    let cache = FsCache::new(map, store, 10);
    assert!(cache.init().is_ok());
}

fn test_fscache_basic(block_size: u64) {
    let data_str = "ABCDEFGHIJKLMN!";
    let mut data = Cursor::new(Vec::from(data_str));
    let filename = OsStr::new("hello.txt");
    let mtime = 1;
    let max_size = Some(100);

    let (cache, map_sneak, store_sneak) = construct_cache(block_size, max_size);
    cache.init().unwrap();

    let map: &TestMap = map_sneak.borrow();
    let store: &TestBucketStore = store_sneak.borrow();

    let fetched: Vec<u8> = cache.fetch(filename, 0, 1024, &mut data, mtime).unwrap();
    assert_eq!(&fetched, data.get_ref());

    let fileblocks = &map.map[filename];
    assert_eq!(fileblocks.mtime, mtime);

    let num_blocks = 1 + ((data_str.len() as u64 - 1) / block_size);
    for i in 0..num_blocks {
        let bucket: u64 = fileblocks.blocks[&i]
                                      .to_str().unwrap()
                                      .parse().unwrap();
        let cached_data: &Vec<u8> = store.buckets[bucket as usize]
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
        eprintln!("block size {}", block_size);
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

    let (cache, map_sneak, store_sneak) = construct_cache(block_size, max_size);
    cache.init().unwrap();

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

#[test]
fn test_fscache_free_orphans() {
    let filler = "ABCDEFGHIJKLMN!";
    let mtime = 1;
    let block_size = filler.len() as u64;
    let num_blocks_per_file = 10u64;
    let max_size = None;
    let filenames = vec!["one", "two", "three", "four", "five"];
    let (cache, mut map_sneak, mut store_sneak) = construct_cache(block_size, max_size);

    let map: &mut TestMap = map_sneak.borrow_mut();
    let store: &mut TestBucketStore = store_sneak.borrow_mut();

    // pre-load the cache with blocks of each of the files.
    for filename in &filenames {
        let osname = OsStr::new(filename);
        map.set_file_mtime(osname, mtime).unwrap();
        for i in 0..num_blocks_per_file {
            let map_path = map.get_block_path(osname, i);
            let bucket = store.put(&map_path, filler.as_bytes(), |path| {
                panic!("unexpected delete of bucket {:?} while writing {:?}/{}",
                    path,
                    osname,
                    i);
            }).unwrap();
            map.put_block(osname, i, &bucket).unwrap();
        }
    }

    cache.init().unwrap();

    // Verify the expected used size.
    assert_eq!(cache.used_size(), filenames.len() as u64 * num_blocks_per_file * block_size);
    // Verify the correct number of buckets were allocated.
    assert_eq!(store.buckets.len(), filenames.len() * num_blocks_per_file as usize);
    // And that no buckets have been freed.
    assert!(store.free_list.is_empty());

    cache.free_orphaned_buckets().unwrap();

    // Nothing should have been freed yet.
    assert!(store.free_list.is_empty());

    map.map.remove(OsStr::new("three"));

    cache.free_orphaned_buckets().unwrap();

    assert_eq!(store.free_list.len() as u64, num_blocks_per_file);
    assert_eq!(store.used_bytes(), (filenames.len() as u64 - 1) * num_blocks_per_file * block_size);
}
