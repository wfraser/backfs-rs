// BackFS Main Entry Point
//
// Copyright (c) 2016 by William R. Fraser
//

use std::env;
use std::ffi::OsString;
use std::fs;
use std::ops::Deref;
use std::path::PathBuf;
use std::process;

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

mod backfs;
use backfs::BackFS;

mod fscache;
mod fsll;
mod inodetable;
mod link;

mod osstrextras;
use osstrextras::OsStrExtras;

extern crate daemonize;
extern crate fuse;
extern crate libc;
extern crate time;
extern crate walkdir;

fn main() {
    let args = env::args_os().collect::<Vec<OsString>>();
    let mut settings = BackfsSettings::parse(&args);

    if settings.cache.is_empty() {
        println!("Error: cache directory not specified. Use the '-o cache=<directory>' option.");
        settings.help = true;
    }

    if settings.verbose {
        println!("{:?}", settings);
    }

    if settings.help {
        println!("{}\nFUSE options:", arg_parse::USAGE);
        settings.fuse_options.push(OsString::from("--help"));
        settings.mount_point = OsString::from(".");  // placate the mount call
    }

    if settings.version {
        print!("{}", backfs::BACKFS_VERSION);
        settings.fuse_options.push(OsString::from("--version"));
        settings.mount_point = OsString::from(".");  // placate the mount call
    }

    if settings.foreground {
        // have FUSE automatically unmount when the process exits.
        settings.fuse_options.push(OsString::from("auto_unmount"));
    } else {
        settings.mount_point = match fs::canonicalize(settings.mount_point) {
            Ok(pathbuf) => pathbuf.into_os_string(),
            Err(e) => {
                println!("error canonicalizing mount point: {}", e);
                process::exit(1);
            }
        };
        settings.backing_fs = match fs::canonicalize(settings.backing_fs) {
            Ok(pathbuf) => pathbuf.into_os_string(),
            Err(e) => {
                println!("error canonicalizing backing filesystem path: {}", e);
                process::exit(1);
            }
        };
        settings.cache = match fs::canonicalize(settings.cache) {
            Ok(pathbuf) => pathbuf.into_os_string(),
            Err(e) => {
                println!("error canonicalizing cache path: {}", e);
                process::exit(1);
            }
        };
    }

    let mut fuse_args: Vec<OsString> = vec![];
    if settings.fuse_options.len() > 0 {
        let mut fuse_options = OsString::new();

        for option in settings.fuse_options.iter() {
            if option.starts_with("-") {
                fuse_args.push(OsString::from(option));
            } else {
                if !fuse_options.is_empty() {
                    fuse_options.push(",");
                }
                fuse_options.push(option);
            }
        }

        if !fuse_options.is_empty() {
            fuse_args.push(OsString::from("-o"));
            fuse_args.push(fuse_options);
        }
    }

    let mountpoint = PathBuf::from(&settings.mount_point);
    let backfs = BackFS::new(settings);

    fuse::mount(backfs, &mountpoint, &fuse_args.as_deref()[..]);
}
