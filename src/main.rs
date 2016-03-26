// BackFS Main Entry Point
//
// Copyright (c) 2016 by William R. Fraser
//

use std::env;
use std::ffi::OsString;
use std::fs;
use std::io;
use std::mem;
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
mod libc_wrappers;
mod link;
mod log_output;

mod osstrextras;
use osstrextras::OsStrExtras;

extern crate daemonize;
extern crate fuse;
extern crate libc;
extern crate syslog;
extern crate time;
extern crate walkdir;

#[macro_use]
extern crate log;

fn redirect_input_to_null() -> io::Result<()> {
    unsafe {
        let fd: libc::c_int = libc::open(mem::transmute(b"/dev/null\0"), libc::O_RDONLY);
        if fd == -1 {
            return Err(io::Error::last_os_error());
        }
        if -1 == libc::dup2(fd, 0) {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

fn main() {
    let args = env::args_os().collect::<Vec<OsString>>();
    let mut settings = BackfsSettings::parse(&args);

    if settings.verbose {
        println!("{:?}", settings);
    }

    if settings.help || settings.version {
        if settings.help {
            println!("{}\nFUSE options:", arg_parse::USAGE);
            settings.fuse_options.push(OsString::from("--help"));
            settings.mount_point = OsString::from(".");  // placate the mount call
            settings.backing_fs = OsString::from(".");
        }

        if settings.version {
            print!("{}", backfs::BACKFS_VERSION);
            settings.fuse_options.push(OsString::from("--version"));
            settings.mount_point = OsString::from(".");  // placate the mount call
            settings.backing_fs = OsString::from(".");
        }
    } else {
        if settings.cache_size != 0 && settings.cache_size < settings.block_size {
            println!("Invalid options: the max cache size cannot be less than the block size.");
            process::exit(-1);
        }

        if settings.cache.is_empty() {
            println!("Invalid options: cache directory not specified. Use the '-o cache=<directory>' option.");
            process::exit(-1);
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

        if settings.verbose {
            // FSLL debug messages aren't very interesting most of the time.
            let filters = vec![("FSLL".to_string(), log::LogLevelFilter::Warn)];
            log_output::init(log::LogLevelFilter::Debug, filters, !settings.foreground)
        } else {
            log_output::init(log::LogLevelFilter::Warn, vec![], !settings.foreground)
        }.unwrap();


        if !settings.foreground {
            // If we're forking to the background, we need to make sure any panics get sent to
            // syslog as well, or we'll never see them.
            // Unfortunately, this is gated on rust nightly for now.
            //log::log_panics();
        }
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

    if let Err(e) = redirect_input_to_null() {
        panic!("Error redirecting stdin to /dev/null: {}", e);
    }

    fuse::mount(backfs, &mountpoint, &fuse_args.as_deref()[..]);
}
