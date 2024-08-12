use std::env;
use std::hint::black_box;
use std::process::exit;
use std::time::Instant;

use anyhow::Context;
use redis::AsyncCommands;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::{ObjectStore, PutPayload};
use object_store::path::Path;

async fn test_test() -> anyhow::Result<()> {
    let redis_dsn = env::var("REDIS_DSN").context("REDIS_DSN not set")?;
    let client = redis::Client::open(redis_dsn)?;
    let mut con = client.get_multiplexed_async_connection().await?;

    let key = "foo";
    let start = Instant::now();
    con.set(key, "bar").await?;
    println!("REDIS set {:?} in {:?}", key, start.elapsed());

    let start = Instant::now();
    let value: Option<String> = con.get(key).await?;
    println!("REDIS get {:?} in {:?} value={:?}", key, start.elapsed(), value);

    let start = Instant::now();
    let value: Option<String> = con.get("missing").await?;
    println!("REDIS get 'missing' in {:?} value={:?}", start.elapsed(), value);

    let key = "big_chunk";
    let start = Instant::now();
    con.set(key, big_chunk()).await?;
    println!("REDIS set '{}' in {:?}", key, start.elapsed());

    let start = Instant::now();
    let value: Option<Vec<u8>> = con.get(key).await?;
    println!("REDIS get '{key}' in {:?} value.is_some() {:?}", start.elapsed(), value.is_some());
    black_box(value);

    Ok(())
}

async fn object_store_test() -> anyhow::Result<()> {
    let gcs_dsn = env::var("GCS_DSN").context("GCS_DSN not set")?;
    let store = GoogleCloudStorageBuilder::from_env().with_url(gcs_dsn).build()?;
    let path = Path::parse(".testing/file.txt")?;
    let payload = PutPayload::from_static(b"this is a test");
    let start = Instant::now();
    store.put(&path, payload).await?;
    println!("GCS put in {:?}", start.elapsed());

    let start = Instant::now();
    let payload = store.get(&path).await?;
    println!("GCS get in {:?}", start.elapsed());

    let start = Instant::now();
    let response = store.get(&Path::parse(".testing/missing.txt")?).await?;
    println!("GCS get missing in {:?}", start.elapsed());
    black_box(response);

    let big_file_path = Path::parse(".testing/big_file.txt")?;
    let payload = PutPayload::from(big_chunk());
    let start = Instant::now();
    store.put(&big_file_path, payload).await?;
    println!("GCS put big file in {:?}", start.elapsed());

    let start = Instant::now();
    let response = store.get(&big_file_path).await?;
    println!("GCS get big file in {:?}", start.elapsed());
    black_box(response);

    Ok(())
}

// 100kb
const BIG_CHUNK_SIZE: usize = 100 * 1024;

fn big_chunk() -> Vec<u8> {
    let mut v = Vec::new();
    for i in 0..BIG_CHUNK_SIZE {
        let byte = (i % 256) as u8;
        v.extend_from_slice(&byte.to_be_bytes());
    }
    v
}

#[tokio::main]
async fn main() {
    if let Err(e) = test_test().await {
        eprintln!("Error: {}", e);
        exit(1);
    }
    if let Err(e) = object_store_test().await {
        eprintln!("Error: {}", e);
        exit(2);
    }
}
