// BackFS command-line argument parsing
//
// Copyright 2016-2018 by William R. Fraser
//
// This is hand-rolled parsing code because none of the available command-line parsing crates I
// found could cope with the odd 'mount'-style arguments this uses (i.e. -o foo, -o bar).
//

use std::borrow::Borrow;
use std::ffi::{OsStr, OsString};
use std::str::FromStr;
use osstrextras::OsStrExtras;

pub const USAGE: &str = "
BackFS.

Usage:
    backfs [-o <option,[option]>] <backing> <mount point>

General Options:
    -h --help         Show this help.
    -V --version      Show the program version.

BackFS Options:
    -o cache            Cache location (REQUIRED)
    -o backing_fs       Backing filesystem location (REQUIRED here or
                            as the first non-option argument)
    -o cache_size       Maximum size for the cache (default is for the cache to
                            grow to fill the device it is on)
    -o rw               Be a read-write cache (default is read-only)
    -o block_size       Cache block size. Defaults to 128K
    -v --verbose        Enable all debugging messages
       -o verbose
    -f --foreground     Enable foreground operation.
       -o foreground
    -d --debug          Enable debugging mode. Same as specifying -v -f.
       -o debug
";

#[derive(Debug)]
pub struct BackfsSettings {
    pub mount_point: OsString,
    pub fuse_options: Vec<OsString>,
    pub help: bool,
    pub version: bool,
    pub cache: OsString,
    pub backing_fs: OsString,
    pub cache_size: u64,
    pub rw: bool,
    pub block_size: u64,
    pub foreground: bool,
    pub verbose: bool,
}

fn parse_human_number(s: &str) -> Result<u64, <u64 as FromStr>::Err> {
    let (multiplier, s) = if s.ends_with('T') {
        (1024 * 1024 * 1024 * 1024, s.trim_right_matches('T'))
    } else if s.ends_with('G') {
        (1024 * 1024 * 1024, s.trim_right_matches('G'))
    } else if s.ends_with('M') {
        (1024 * 1024, s.trim_right_matches('M'))
    } else if s.ends_with('K') {
        (1024, s.trim_right_matches('K'))
    } else {
        (1, s)
    };

    match s.parse::<u64>() {
        Ok(n) => Ok(n * multiplier),
        Err(e) => Err(e)
    }
}

impl BackfsSettings {
    pub fn parse(args: &[OsString]) -> BackfsSettings {

        // These are the default settings:
        let mut settings = BackfsSettings {
            mount_point: OsString::new(),
            fuse_options: vec![],
            help: false,
            version: false,
            cache: OsString::new(),
            backing_fs: OsString::new(),
            cache_size: 0,
            rw: false,
            block_size: 0x20_000,   // 131072 = 128 KiB
            foreground: false,
            verbose: false
        };

        let mut options: Vec<OsString> = vec![];
        let mut values: Vec<OsString> = vec![];
        let mut is_opt = false;
        let mut parsing_options = true;
        let mut first = true;
        for arg in args {
            // skip the program name
            if first {
                first = false;
                continue;
            }

            if parsing_options {
                if arg == "-o" {
                    is_opt = true;
                    continue;
                }

                if is_opt {
                    for option in arg.split(b',') {
                        options.push(option.to_os_string());
                    }
                    is_opt = false;
                } else if arg == "--" {
                    parsing_options = false;
                } else if arg == "-h" || arg == "--help" {
                    options.push(OsString::from("help"));
                } else if arg == "-V" || arg == "--version" {
                    options.push(OsString::from("version"));
                } else if arg == "-v" || arg == "--verbose" {
                    options.push(OsString::from("verbose"));
                } else if arg == "-f" || arg == "--foreground" {
                    options.push(OsString::from("foreground"));
                } else if arg == "-d" || arg == "--debug" {
                    options.push(OsString::from("foreground"));
                    options.push(OsString::from("verbose"));
                } else if arg.starts_with(&"-") {
                    println!("unrecognized option \"{:?}\"", arg);
                    options.push(OsString::from("help"));
                    break;
                } else {
                    parsing_options = false;
                    values.push(arg.clone());
                }
            } else {
                values.push(arg.clone());
            }
        }

        // now interpret the options and values
        for opt in options {
            let parts: Vec<&OsStr> = opt.splitn(2, b'=').collect();
            if parts.len() == 2 {
                match parts[0].to_str() {
                    Some("cache") => settings.cache = parts[1].to_os_string(),
                    Some("backing_fs") => settings.backing_fs = parts[1].to_os_string(),
                    Some("cache_size") => match parse_human_number(parts[1].to_string_lossy().borrow()) {
                        Ok(n) => { settings.cache_size = n; },
                        Err(e) => {
                            println!("invalid cache size: {}", e);
                            settings.help = true;
                        }
                    },
                    Some("block_size") => match parse_human_number(parts[1].to_string_lossy().borrow()) {
                        Ok(n) => { settings.block_size = n; },
                        Err(e) => {
                            println!("invalid block size: {}", e);
                            settings.help = true;
                        }
                    },
                    _ => settings.fuse_options.push(parts[1].to_os_string())
                }
            } else {
                match opt.to_str() {
                    Some("help") => settings.help = true,
                    Some("version") => settings.version = true,
                    Some("rw") => settings.rw = true,
                    Some("verbose") => settings.verbose = true,
                    Some("foreground") => settings.foreground = true,
                    _ => settings.fuse_options.push(opt.to_os_string())
                }
            }
        }

        match values.len() {
            1 => {
                if settings.backing_fs.is_empty() {
                    println!("required backing filesystem argument not specified!");
                    settings.help = true;
                } else {
                    settings.mount_point = values[0].clone();
                }
            },
            2 => {
                settings.backing_fs = values[0].clone();
                settings.mount_point = values[1].clone();
            },
            _ => {
                if !settings.help && !settings.version {
                    println!("invalid number of non-option arguments given.");
                    settings.help = true;
                }
            }
        }

        settings
    }
}
