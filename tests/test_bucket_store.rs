// BackFS Filesystem Tests :: Cache Block Store
//
// Copyright (c) 2016 by William R. Fraser
//

use std::collections::LinkedList;
use std::ffi::{OsStr, OsString};
use std::io;
use std::ops::IndexMut;

extern crate libc;

extern crate backfs_rs;
use backfs_rs::bucket_store::*;

struct TestBucket {
    data: Option<Vec<u8>>,
}

pub struct TestBucketStore {
    buckets: Vec<TestBucket>,
    used_list: LinkedList<usize>,
    free_list: LinkedList<usize>,
    used_bytes: u64,
    max_bytes: Option<u64>,
}

fn parse_path(path: &OsStr) -> usize {
    path.to_str().unwrap().parse().unwrap()
}

fn list_disconnect<T>(list: &mut LinkedList<T>, index: usize) -> T {
    let mut after: LinkedList<T> = list.split_off(index);
    let elem = list.pop_back().unwrap();
    list.append(&mut after);
    elem
}

impl TestBucketStore {
    pub fn new(max_bytes: Option<u64>) -> TestBucketStore {
        TestBucketStore {
            buckets: vec![],
            used_list: LinkedList::new(),
            free_list: LinkedList::new(),
            used_bytes: 0,
            max_bytes: max_bytes,
        }
    }
}

impl CacheBucketStore for TestBucketStore {
    fn init<F>(&mut self, mut _delete_handler: F) -> io::Result<()>
            where F: FnMut(&OsStr) -> io::Result<()> {
        Ok(())
    }

    fn get(&self, bucket_path: &OsStr) -> io::Result<Vec<u8>> {
        match self.buckets[parse_path(bucket_path)].data {
            Some(ref data) => Ok(data.clone()),
            None => Err(io::Error::from_raw_os_error(libc::ENOENT)),
        }
    }

    fn put<F>(&mut self, data: &[u8], mut delete_handler: F) -> io::Result<OsString>
            where F: FnMut(&OsStr) -> io::Result<()> {
        while self.max_bytes.is_some() && self.used_bytes + data.len() as u64 > self.max_bytes.unwrap() {
            let (bucket_path, _) = self.delete_something().unwrap();
            if let Err(e) = delete_handler(&bucket_path) {
                return Err(e);
            }
        }

        let index = if self.free_list.is_empty() {
            self.buckets.push(TestBucket { data: None });
            self.buckets.len() - 1
        } else {
            self.free_list.pop_front().unwrap()
        };

        self.used_list.push_front(index);

        self.buckets.index_mut(index).data = Some(Vec::from(data));

        Ok(OsString::from(format!("{}", index)))
    }

    fn free_bucket(&mut self, bucket_path: &OsStr) -> io::Result<u64> {
        let number = parse_path(bucket_path);

        {
            // This is inefficient, but it's test code, so IDGAF.
            let pos = self.used_list.iter().position(|x| x == &number).unwrap();
            list_disconnect(&mut self.used_list, pos);
            self.free_list.push_front(number);
        }

        let mut bucket = self.buckets.index_mut(number);
        let n = bucket.data.as_ref().unwrap().len() as u64;
        bucket.data = None;

        self.used_bytes -= n;
        Ok(n)
    }

    fn delete_something(&mut self) -> io::Result<(OsString, u64)> {
        let number = self.used_list.pop_back().unwrap();
        self.free_list.push_front(number);

        let mut bucket = self.buckets.index_mut(number);
        let n = bucket.data.as_ref().unwrap().len() as u64;
        bucket.data = None;

        self.used_bytes -= n;
        Ok((OsString::from(format!("{}", number)), n))
    }

    fn used_bytes(&self) -> u64 {
        self.used_bytes
    }

    fn max_bytes(&self) -> Option<u64> {
        self.max_bytes
    }
}
