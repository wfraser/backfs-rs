// BackFS Log Output
//
// Copyright 2016-2020 by William R. Fraser
//

use std::boxed::Box;
use std::sync::Mutex;
use syslog::{Facility, Formatter3164, Logger, LoggerBackend};

struct Log {
    global_filter: log::LevelFilter,
    target_filter: Vec<(String, log::LevelFilter)>,
    syslog: Option<Mutex<Logger<LoggerBackend, Formatter3164>>>,
}

pub fn init(global_filter: log::LevelFilter,
            target_filter: Vec<(String, log::LevelFilter)>,
            use_syslog: bool)
    -> Result<(), log::SetLoggerError>
{
    log::set_max_level(global_filter);

    let formatter = Formatter3164 {
        facility: Facility::LOG_USER,
        process: "backfs".into(),
        hostname: None,
        pid: 0,
    };

    let syslog = if use_syslog {
        match syslog::unix(formatter) {
            Ok(writer) => Some(Mutex::new(writer)),
            Err(e) => {
                println!("Error opening connection to syslog: {}", e);
                println!("Logging disabled!");
                None
            }
        }
    } else {
        None
    };

    log::set_boxed_logger(Box::new(Log {
        global_filter,
        target_filter,
        syslog,
    }))
}

impl log::Log for Log {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        if self.global_filter < metadata.level() {
            return false;
        }

        for (target, filter) in &self.target_filter {
            if metadata.target() == target && filter < &metadata.level() {
                return false;
            }
        }

        true
    }

    fn log(&self, record: &log::Record<'_>) {
        if self.enabled(record.metadata()) {
            if let Some(ref syslog) = self.syslog {
                let mut syslog = syslog.lock().unwrap();
                let msg = format!("{}: {}", record.target(), record.args());
                use log::Level::*;
                let _ = match record.level() {
                    Error => syslog.err(msg),
                    Warn => syslog.warning(msg),
                    Info => syslog.info(msg),
                    Debug => syslog.debug(msg),
                    Trace => syslog.debug(msg),
                };
            } else {
                println!("{}: {}: {}", record.target(), record.level(), record.args());
            }
        }
    }

    fn flush(&self) {}
}
