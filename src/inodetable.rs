// InodeTable :: a bi-directional map for persistent path <-> inode storage.
//
// Copyright (c) 2016 by William R. Fraser
//
// As BackFS needs to generate paths, each one will get its own unique inode number that will live
// for the duration of the mount. These are not persisted anywhere (on unmount, they go away).

use std::collections::BTreeMap;
use std::collections::btree_map::Entry::*;
use std::ffi::OsString;
use std::rc::Rc;

type Inode = u64;

pub struct InodeTable {
    map: BTreeMap<Rc<OsString>, Inode>,
    table: Vec<Rc<OsString>>
}

impl InodeTable {    
    pub fn new() -> InodeTable {
        InodeTable {
            map: BTreeMap::new(),
            table: Vec::new()
        }
    }
    
    pub fn add(&mut self, path: OsString) -> Inode {
        let inode = (self.table.len() + 1) as Inode; // inodes will start at 1
        let rc = Rc::new(path);
        match self.map.insert(rc.clone(), inode) {
            Some(_) => { panic!("duplicate path inserted into inode table!"); },
            None    => ()
        }
        self.table.push(rc);
        inode
    }

    pub fn add_or_get(&mut self, path: Rc<OsString>) -> Inode {
        match self.map.entry(path.clone()) {
            Vacant(entry) => {
                let inode = (self.table.len() + 1) as Inode;
                entry.insert(inode);
                self.table.push(path);
                inode
            },
            Occupied(entry) => {
                *entry.get()
            }
        }
    }
    
    pub fn get_path(&self, inode: Inode) -> Option<Rc<OsString>> {
        match self.table.get((inode - 1) as usize) {
            Some(rc) => Some(rc.clone()),
            None     => None
        }
    }
    
    pub fn get_inode(&self, path: &OsString) -> Option<Inode> {
        match self.map.get(path) {
            Some(inode) => Some(*inode),
            None        => None
        }
    }
}
