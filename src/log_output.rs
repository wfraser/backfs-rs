// BackFS Log Output
//
// Copyright (c) 2016 by William R. Fraser
//

use std::boxed::Box;
use log;
use log::LogLevel;
use syslog;
use syslog::{Facility, Logger, Severity};

struct Log {
    global_filter: log::LogLevelFilter,
    target_filter: Vec<(String, log::LogLevelFilter)>,
    syslog: Option<Box<Logger>>,
}

pub fn init(global_filter: log::LogLevelFilter,
            target_filter: Vec<(String, log::LogLevelFilter)>,
            use_syslog: bool)
    -> Result<(), log::SetLoggerError> {
    log::set_logger(|max_log_level| {
        max_log_level.set(global_filter);

        let maybe_syslog = if use_syslog {
            match syslog::unix(Facility::LOG_USER) {
                Ok(writer) => Some(writer),
                Err(e) => {
                    println!("Error opening connection to syslog: {}", e);
                    println!("Logging disabled!");
                    None
                }
            }
        } else {
            None
        };

        Box::new(Log {
            global_filter: global_filter,
            target_filter: target_filter,
            syslog: maybe_syslog,
        })
    })
}

fn loglevel_to_syslog_severity(level: LogLevel) -> Severity {
    match level {
        LogLevel::Error => Severity::LOG_ERR,
        LogLevel::Warn  => Severity::LOG_WARNING,
        LogLevel::Info  => Severity::LOG_INFO,
        LogLevel::Debug => Severity::LOG_DEBUG,
        LogLevel::Trace => Severity::LOG_DEBUG,
    }
}

impl log::Log for Log {
    fn enabled(&self, metadata: &log::LogMetadata) -> bool {
        if self.global_filter < metadata.level() {
            return false;
        }

        for &(ref target, ref filter) in &self.target_filter {
            if metadata.target() == target {
                if *filter < metadata.level() {
                    return false;
                }
            }
        }

        true
    }

    fn log(&self, record: &log::LogRecord) {
        if self.enabled(record.metadata()) {
            let msg = format!("backfs: {}: {}: {}", record.target(), record.level(), record.args());
            if let Some(ref syslog) = self.syslog {
                let severity = loglevel_to_syslog_severity(record.level());
                let _errors_ignored = syslog.send(severity, &msg);
            } else {
                println!("{}", msg);
            }
        }
    }
}
