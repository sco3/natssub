use env_logger::Env;
use futures::StreamExt;

use log::info;
use log::{debug, error};

use async_nats::jetstream::consumer::pull::Config;
use async_nats::jetstream::consumer::{Consumer, DeliverPolicy};
use std::env::args;
use std::error::Error;
use std::io::Bytes;
use tokio;

fn help(s: &str) -> String {
    println!("Help: natssub <subject> <stream> [<nats_url>]");
    s.to_string()
}

async fn recv() -> Result<(), Box<dyn Error + Send + Sync>> {
    let subject = args()
        .nth(1)
        .ok_or_else(|| help("Param not found : subject"))?;

    let stream = args()
        .nth(2)
        .ok_or_else(|| help("Param not found: stream"))?;

    let url = args().nth(3).unwrap_or("nats://localhost:4222".to_string());

    info!(
        "Subscribe to subject {} from stream {} url {}",
        subject, stream, url
    );

    let client = async_nats::connect(&url).await?;
    let jetstream = async_nats::jetstream::new(client);

    let mut subjects = vec![subject.to_string()];
    let mut stream_config = async_nats::jetstream::stream::Config {
        name: stream.to_string(),
        subjects: subjects.clone(),
        ..Default::default()
    };
    let stream = jetstream
        .get_or_create_stream(stream_config.clone())
        .await?;

    let stream_info = stream.get_info().await?;

    for existing_subject in stream_info.config.subjects {
        if !subjects.contains(&existing_subject) {
            subjects.push(existing_subject);
        }
    }
    stream_config.subjects = subjects;

    //jetstream.update_stream(stream_config).await?;

    let durable = String::from(format!("consumer_{subject}"));
    let config = async_nats::jetstream::consumer::pull::Config {
        durable_name: Some(durable.clone()),
        deliver_policy: DeliverPolicy::New,
        filter_subject: subject.to_string(),
        ..Default::default()
    };
    let consumer = stream
        .get_or_create_consumer(
            durable.as_str(),
            config.clone(), //
        )
        .await?;
    serve(consumer).await;
    Ok(())
}

async fn serve(consumer: Consumer<Config>) {
    loop {
        match consumer.fetch().max_messages(1).messages().await {
            Ok(mut messages) => {
                while let Some(Ok(message)) = messages.next().await {
                    let payload = &message.payload;
                    let ts = message.info().unwrap().published;
                    let nanos = ts.unix_timestamp_nanos() / 1000000;
                    println!("{} {}", nanos, String::from_utf8_lossy(payload.as_ref()));
                }
            }
            Err(e) => {
                error!("Failed to fetch messages: {}", e);
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let level = Env::default().default_filter_or("info");
    env_logger::init_from_env(level);
    match recv().await {
        Ok(_) => {
            println!("Done");
        }
        Err(e) => {
            error!("Error: {}", e);
        }
    }
}
