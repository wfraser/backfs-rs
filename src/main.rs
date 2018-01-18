// BackFS Main Entry Point
//
// Copyright 2016-2018 by William R. Fraser
//

#![allow(unknown_lints)]

#![allow(unit_expr)] // https://github.com/rust-lang-nursery/rust-clippy/issues/2095

extern crate backfs;
use backfs::BackFS;
use backfs::arg_parse::{self, BackfsSettings};

extern crate fuse_mt;
use fuse_mt::{FuseMT, FilesystemMT};

extern crate libc;
extern crate log;
extern crate log_panics;
extern crate syslog;

mod log_output;

mod osstrextras;
use osstrextras::OsStrExtras;

use std::env;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::io;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::process;

trait VecDeref<T: Deref> {
    fn as_deref(&self) -> Vec<&T::Target>;
}

impl<T: Deref> VecDeref<T> for Vec<T> {
    fn as_deref(&self) -> Vec<&T::Target> {
        self.iter().map(Deref::deref).collect()
    }
}

fn redirect_input_to_null() -> io::Result<()> {
    unsafe {
        let fd: libc::c_int = libc::open(b"/dev/null\0" as *const u8 as *const libc::c_char,
                                         libc::O_RDONLY);
        if fd == -1 {
            return Err(io::Error::last_os_error());
        }
        if -1 == libc::dup2(fd, 0) {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

fn mount_and_exit<FS, P>(fs: FS, num_threads: usize, path: &P, options: &[&OsStr]) -> !
        where FS: FilesystemMT + Sync + Send + 'static,
              P: AsRef<Path> {
    if let Err(e) = redirect_input_to_null() {
        panic!("Error redirecting stdin to /dev/null: {}", e);
    }

    fuse_mt::mount(FuseMT::new(fs, num_threads), path, options).unwrap();
    process::exit(0);
}

fn main() {
    let args = env::args_os().collect::<Vec<OsString>>();
    let mut settings = BackfsSettings::parse(&args);

    if settings.verbose {
        println!("{:?}", settings);
    }

    if settings.help || settings.version {
        let mut options = Vec::<&OsStr>::new();

        if settings.help {
            println!("{}\nFUSE options:", arg_parse::USAGE);
            options.push(OsStr::new("--help"));
        } else if settings.version {
            println!("BackFS version: {} {}\nFuseMT version: {}",
                     backfs::VERSION, backfs::GIT_REVISION, fuse_mt::VERSION);
            options.push(OsStr::new("--version"));
        }

        struct DummyFS;
        impl fuse_mt::FilesystemMT for DummyFS {
        }

        mount_and_exit(DummyFS, 1, &Path::new("."), &options);
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
            log_panics::init();
        }
    }

    let mut fuse_args: Vec<OsString> = vec![];
    if !settings.fuse_options.is_empty() {
        let mut fuse_options = OsString::new();

        for option in &settings.fuse_options {
            if option.starts_with(&"-") {
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

    mount_and_exit(backfs, 1, &mountpoint, &fuse_args.as_deref()[..]);
}
