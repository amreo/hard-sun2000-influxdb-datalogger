#![feature(proc_macro_hygiene, decl_macro)]

extern crate ctrlc;
extern crate simplelog;
use simplelog::*;

extern crate ini;
use self::ini::Ini;

use futures::future::join_all;
use humantime::format_duration;
use std::env;
use std::fs::OpenOptions;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc};
use std::time::{Duration, Instant};
use tokio::task;
use tokio_compat_02::FutureExt;

mod sun2000;

fn get_config_string(option_name: &str, section: Option<&str>) -> Option<String> {
    
    let conf = Ini::load_from_file("hard.conf").expect("Cannot open config file");
    conf.section(Some(section.unwrap_or("general").to_owned()))
        .and_then(|x| x.get(option_name).cloned())
}

fn get_config_bool(option_name: &str, section: Option<&str>) -> bool {
    let conf = Ini::load_from_file("hard.conf").expect("Cannot open config file");
    let value = conf
        .section(Some(section.unwrap_or("general").to_owned()))
        .and_then(|x| x.get(option_name).cloned());
    match value {
        Some(val) => match val.trim() {
            "yes" => true,
            "true" => true,
            "1" => true,
            _ => false,
        },
        _ => false,
    }
}

fn logging_init() {
    let conf = ConfigBuilder::new()
        .set_time_format("%F, %H:%M:%S%.3f".to_string())
        .set_write_log_enable_colors(true)
        .build();

    let mut loggers = vec![];

    let console_logger: Box<dyn SharedLogger> = TermLogger::new(
        LevelFilter::Info,
        conf.clone(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    );
    loggers.push(console_logger);

    let mut logfile_error: Option<String> = None;
    match get_config_string("log", None) {
        Some(ref log_path) => {
            let logfile = OpenOptions::new().create(true).append(true).open(log_path);
            match logfile {
                Ok(logfile) => {
                    loggers.push(WriteLogger::new(LevelFilter::Info, conf, logfile));
                }
                Err(e) => {
                    logfile_error = Some(format!(
                        "Error creating/opening log file: {:?}: {:?}",
                        log_path, e
                    ));
                }
            }
        }
        _ => {}
    };

    CombinedLogger::init(loggers).expect("Cannot initialize logging subsystem");
    if logfile_error.is_some() {
        error!("{}", logfile_error.unwrap());
        warn!("Will do console logging only...");
    }
}

#[tokio::main]
async fn main() {
    env::set_var("RUST_BACKTRACE", "full");
    let started = Instant::now();
    logging_init();
    info!("🛡️ Welcome to hard (home automation rust-daemon)");

    //Ctrl-C / SIGTERM support
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");

    //common thread stuff
    let influxdb_url = get_config_string("influxdb_url", None);
    let mut futures = vec![];
    let cancel_flag = Arc::new(AtomicBool::new(false));

    //sun2000 async task
    match get_config_string("host", Some("sun2000")) {
        Some(host) => {
            let worker_cancel_flag = cancel_flag.clone();
            let mut sun2000 = sun2000::Sun2000 {
                name: "sun2000".to_string(),
                host_port: host,
                poll_ok: 0,
                poll_errors: 0,
                influxdb_url: influxdb_url.clone(),
                mode_change_script: get_config_string("mode_change_script", Some("sun2000")),
                optimizers: get_config_bool("optimizers", Some("sun2000")),
                battery_installed: get_config_bool("battery_installed", Some("sun2000")),
                dongle_connection: get_config_bool("dongle_connection", Some("sun2000")),
            };
            let sun2000_future =
                task::spawn(async move { sun2000.worker(worker_cancel_flag).compat().await });
            futures.push(sun2000_future);
        }
        _ => {}
    }

    debug!("Entering main loop...");
    loop {
        if !running.load(Ordering::SeqCst) {
            info!("🛑 Ctrl-C or SIGTERM signal detected, exiting...");
            break;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    info!("🏁 Stopping all threads...");
    //inform all threads about termination
    cancel_flag.store(true, Ordering::SeqCst);
    //wait for tokio async tasks
    let _ = join_all(futures).await;

    info!(
        "🚩 hard terminated, daemon running time: {}",
        format_duration(started.elapsed()).to_string()
    );
}
