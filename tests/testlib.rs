// BackFS Component Tests
//
// Copyright (c) 2016 by William R. Fraser
//

extern crate backfs_rs;
use backfs_rs::fscache::*;

mod test_block_map;
use test_block_map::*;
mod test_bucket_store;
use test_bucket_store::*;

#[test]
fn test_fscache_init() {
    let map = TestMap::new();
    let store = TestBucketStore::new(Some(100));
    let mut cache = FSCache::new(map, store, 10);
    assert!(cache.init().is_ok());
}
