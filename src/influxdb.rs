use std::{sync::{Arc, atomic::{AtomicBool, Ordering}}, time::Duration};

use influxdb::{WriteQuery, Client};
use simplelog::*;
use tokio::sync::mpsc::Receiver;

// Just a generic Result type to ease error handling for us. Errors in multithreaded
// async contexts needs some extra restrictions
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub struct InfluxdbWriter {
    pub name: String,
    pub influxdb_url: Option<String>,
    pub influxdb_token: Option<String>,
    pub rx_influxdb: Receiver<Vec<WriteQuery>>,
}

impl InfluxdbWriter {
    pub async fn worker(&mut self, worker_cancel_flag: Arc<AtomicBool>) -> Result<()> {
        info!("{}: Starting task", self.name);

        // let mut terminated = false;

        loop {
            if worker_cancel_flag.load(Ordering::SeqCst) {
                break;
            }

            let client = match &self.influxdb_url {
                Some(url) => match &self.influxdb_token {
                    Some(token) => Some(Client::new(url, "sun2000").with_token(token)),
                    None => Some(Client::new(url, "sun2000")),
                },
                None => None,
            };    

            let task = self.rx_influxdb.try_recv();
            match task {
                Ok(t) => {
                    debug!(
                        "{}: received vector {:?}",
                        self.name, t
                    );

                    if let Some(c) = client.clone() {
                        let _ = save_multiple_to_influxdb(c, &self.name, t).await;
                    }
                },
                _ => (),
            }

            tokio::time::sleep(Duration::from_millis(30)).await;
        }

        info!("{}: task stopped", self.name);
        Ok(())
    }
}

async fn save_multiple_to_influxdb(
    client: influxdb::Client,
    thread_name: &String,
    query: Vec<WriteQuery>,
) -> Result<()> {
    match client.query(query).await {
        Ok(msg) => {
            if msg != "" {
                error!("{}: influxdb write success: {:?}", thread_name, msg);
            } else {
                debug!("{}: influxdb write success: {:?}", thread_name, msg);
            }
        }
        Err(e) => {
            error!("<i>{}</>: influxdb write error: <b>{:?}</>", thread_name, e);
        }
    }        

    Ok(())
}
