// BackFS Main Entry Point
//
// Copyright (c) 2016 by William R. Fraser
//

use std::env;
use std::ffi::OsString;
use std::ops::Deref;
use std::path::Path;

trait VecDeref<T: Deref> {
    fn as_deref(&self) -> Vec<&T::Target>;
}

impl<T: Deref> VecDeref<T> for Vec<T> {
    fn as_deref(&self) -> Vec<&T::Target> {
        self.iter().map(Deref::deref).collect()
    }
}

mod arg_parse;
use arg_parse::BackfsSettings;

mod inodetable;

mod backfs;
use backfs::BackFS;

extern crate libc;
use libc::*;

extern crate fuse;

fn main() {    
    let args = env::args().collect::<Vec<String>>();
    let mut settings = BackfsSettings::parse(&args);
    
    if settings.verbose {
        println!("{:?}", settings);
    }
    
    if settings.help {
        println!("{}\nFUSE options:", arg_parse::USAGE);
        settings.fuse_options.push("--help");
        settings.mount_point = ".";  // placate the mount call
    }
    
    if settings.version {
        println!("BackFS version: 0.1.0");
        settings.fuse_options.push("--version");
        settings.mount_point = ".";  // placate the mount call
    }
    
    if settings.foreground {
        // have FUSE automatically unmount when the process exits.
        settings.fuse_options.push("auto_unmount");
    }
    
    let mut fuse_args: Vec<OsString> = vec![];
    if settings.fuse_options.len() > 0 {
        let mut fuse_options = "".to_string();
        
        for option in settings.fuse_options.iter() {
            if option.starts_with("-") {
                fuse_args.push(OsString::from(option));
            } else {
                if !fuse_options.is_empty() {
                    fuse_options.push_str(",");
                }
                fuse_options.push_str(option);
            }
        }
        
        if !fuse_options.is_empty() {
            fuse_args.push(OsString::from("-o"));
            fuse_args.push(OsString::from(fuse_options));
        }
    }
    
    let mountpoint = Path::new(settings.mount_point);
    
    let backfs = BackFS::new(settings);
    
    // TODO: need to fork to background before doing this, unless backfs.settings.foreground is specified.
    fuse::mount(backfs, &mountpoint, &fuse_args.as_deref()[..]);
}
