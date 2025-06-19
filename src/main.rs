use env_logger::Env;
use futures::StreamExt;
use std::collections::HashSet;

use log::{debug, error};
use log::{info, warn};

use async_nats::jetstream::consumer::pull::Config;
use async_nats::jetstream::consumer::Consumer;
use async_nats::jetstream::stream::Stream;
use std::env::args;
use std::time::Duration;

fn help(s: &str) -> String {
    println!("Help: natssub <subject> <stream> [<nats_url>]");
    s.to_string()
}

fn merge_unique(vec1: Vec<String>, vec2: Vec<String>) -> Vec<String> {
    let mut set: HashSet<String> = HashSet::new();
    set.extend(vec1);
    set.extend(vec2);
    set.into_iter().collect()
}

async fn recv() /*-> Result<(), Box<dyn Error + Send + Sync>>*/
{
    let subject = args()
        .nth(1)
        .ok_or_else(|| help("Param not found : subject"))
        .unwrap();

    let stream = args()
        .nth(2)
        .ok_or_else(|| help("Param not found: stream"))
        .unwrap();

    let url = args().nth(3).unwrap_or("nats://localhost:4222".to_string());

    info!("Subscribe to subject {subject} from stream {stream} url {url}",);

    match async_nats::connect(&url).await {
        Ok(client) => {
            let jetstream = async_nats::jetstream::new(client);

            let subjects = vec![subject.to_string()];
            let mut stream_config = async_nats::jetstream::stream::Config {
                name: stream.to_string(),
                subjects: subjects.clone(),
                ..Default::default()
            };
            match jetstream.get_stream(&stream).await {
                Ok(mut s) => match s.get_info().await {
                    Ok(info) => {
                        let mut needs_update = false;
                        for subject in subjects.clone() {
                            if !stream_config.subjects.contains(&subject) {
                                needs_update = true;
                            }
                        }
                        if !needs_update {
                            info!("Stream {stream} with subjects: {subjects:?} is ready.");
                            continue_with_stream(subject.clone(), &mut s).await;
                            // return
                        }

                        stream_config = info.config;
                        let upd_subjects = merge_unique(
                            subjects.clone(), //
                            stream_config.subjects,
                        );
                        stream_config.subjects = upd_subjects;
                    }
                    Err(e) => {
                        error!(
                            "Failed to get stream info: {:?}, {}",
                            e, "using defaults for stream update.",
                        );
                    }
                },
                Err(e) => {
                    info!("Stream not found {e:?}, trying create it.");
                }
            }
            // update or create stream
            match jetstream.get_or_create_stream(stream_config.clone()).await {
                Ok(mut stream) => match stream.get_info().await {
                    Ok(info) => {
                        let upd_subjects = merge_unique(subjects, info.config.subjects);
                        stream_config.subjects = upd_subjects;

                        match jetstream.update_stream(stream_config).await {
                            Ok(updated) => {
                                debug!("Stream updated: {updated:?}");
                                continue_with_stream(subject, &mut stream).await;
                                //return;
                            }
                            Err(err) => {
                                error!("Failed to update stream: {err:?}");
                            }
                        }
                    }
                    Err(err) => {
                        error!("Failed to get stream info: {err:?}");
                    }
                },
                Err(err) => {
                    error!("Failed to get or create stream: {err:?}");
                }
            }
        }
        Err(e) => {
            error!("Connect error: {e:?}");
        }
    }
}

/*
       let consumer = stream
           .get_or_create_consumer(
               &durable_name,
               Config {
                   durable_name: Some(durable_name.clone()),
                   filter_subjects: vec![subj.clone()],
                   ..Default::default()
               },
           )
           .await
           .map_err(|e| {
               error!("Failed to create consumer: {}", e);
               e
           })?;

*/

async fn continue_with_stream(subject: String, js: &mut Stream) {
    let durable_safe = subject.replace('.', "_");
    let durable = format!("consumer_{durable_safe}");

    match js.get_consumer::<Config>(durable.as_str()).await {
        Ok(c) => {
            info!("Consumer found");
            info!("Created {c:?}");
            serve(c).await;
        }
        Err(e) => {
            warn!("Consumer not found: {durable} {e:?}");
            let config = async_nats::jetstream::consumer::pull::Config {
                durable_name: Some(durable.clone()),
                filter_subject: subject.to_string(),
                ..Default::default()
            };
            match js.create_consumer(config).await {
                Ok(c) => {
                    info!("Created {c:?}");
                    serve(c).await;
                }
                Err(e) => {
                    error!("Consumer error1 {e:?}");
                }
            }
        }
    }
}

async fn serve(consumer: Consumer<Config>) {
    loop {
        //info!(".");
        match consumer
            .fetch() //
            .max_messages(1)
            .expires(Duration::from_secs(1))
            .messages()
            .await
        {
            Ok(mut messages) => {
                //info!("..");
                while let Some(Ok(message)) = messages.next().await {
                    info!("...");
                    let payload = &message.payload;
                    let ts = message.info().unwrap().published;
                    let nanos = ts.unix_timestamp_nanos() / 1_000_000;
                    println!("{} {}", nanos, String::from_utf8_lossy(payload.as_ref()));
                }
            }
            Err(e) => {
                error!("Failed to fetch messages: {e:?}");
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let level = Env::default().default_filter_or("info");
    env_logger::init_from_env(level);
    Box::pin(recv()).await;
}
