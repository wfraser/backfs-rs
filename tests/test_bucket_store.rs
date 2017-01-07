// BackFS Filesystem Tests :: Cache Block Store
//
// Copyright (c) 2016 by William R. Fraser
//

use std::collections::LinkedList;
use std::ffi::{OsStr, OsString};
use std::io;
use std::ops::IndexMut;

extern crate libc;

extern crate backfs;
use backfs::bucket_store::*;

pub struct TestBucket {
    pub data: Option<Vec<u8>>,
    pub parent: Option<OsString>,
}

pub struct TestBucketStore {
    pub buckets: Vec<TestBucket>,
    pub used_list: LinkedList<usize>,
    pub free_list: LinkedList<usize>,
    pub used_bytes: u64,
    pub max_bytes: Option<u64>,
}

fn parse_path(path: &OsStr) -> usize {
    path.to_str().unwrap().parse().unwrap()
}

fn list_disconnect<T>(list: &mut LinkedList<T>, index: usize) -> T {
    let mut after: LinkedList<T> = list.split_off(index);
    let elem = after.pop_front().unwrap();
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

    fn put<F>(&mut self, parent: &OsStr, data: &[u8], mut delete_handler: F) -> io::Result<OsString>
            where F: FnMut(&OsStr) -> io::Result<()> {
        while self.max_bytes.is_some() && self.used_bytes + data.len() as u64 > self.max_bytes.unwrap() {
            let (bucket_path, _) = self.delete_something().unwrap();
            if let Err(e) = delete_handler(&bucket_path) {
                return Err(e);
            }
        }

        let index = if self.free_list.is_empty() {
            self.buckets.push(TestBucket { data: None, parent: Some(parent.to_os_string()) });
            self.buckets.len() - 1
        } else {
            self.free_list.pop_front().unwrap()
        };

        self.used_list.push_front(index);

        self.buckets.index_mut(index).data = Some(Vec::from(data));
        self.used_bytes += data.len() as u64;

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
        let parent = ::std::mem::replace(&mut bucket.parent, None);

        self.used_bytes -= n;
        Ok((parent.unwrap(), n))
    }

    fn used_bytes(&self) -> u64 {
        self.used_bytes
    }

    fn max_bytes(&self) -> Option<u64> {
        self.max_bytes
    }

    fn enumerate_buckets<F>(&self, mut handler: F) -> io::Result<()>
            where F: FnMut(&OsStr, Option<&OsStr>) -> io::Result<()> {
        for i in 0 .. self.buckets.len() {
            let path = format!("{}", i);
            let parent_opt = &self.buckets[i].parent;
            let parent_opt_ref = parent_opt.as_ref().map(|x| x.as_ref());
            handler(&OsStr::new(&path), parent_opt_ref).unwrap();
        }
        Ok(())
    }
}
