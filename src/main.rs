use std::env;
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

    println!("setting 'foo'...");
    let start = Instant::now();
    con.set("foo", "bar").await?;
    println!("set 'foo' in {:?}", start.elapsed());

    println!("getting 'foo'...");
    let start = Instant::now();
    let value: Option<String> = con.get("foo").await?;
    println!("get 'foo' in {:?} value={:?}", start.elapsed(), value);

    println!("getting 'missing' key...");
    let start = Instant::now();
    let value: Option<String> = con.get("missing").await?;
    println!("get 'missing' in {:?} value={:?}", start.elapsed(), value);
    Ok(())
}

async fn object_store_test() -> anyhow::Result<()> {
    let gcs_dsn = env::var("GCS_DSN").context("GCS_DSN not set")?;
    let store = GoogleCloudStorageBuilder::from_env().with_url(gcs_dsn).build()?;
    let path = Path::parse(".testing/file.txt")?;
    let payload = PutPayload::from_static(b"this is a test");
    let start = Instant::now();
    store.put(&path, payload).await?;
    println!("put in {:?}", start.elapsed());

    let start = Instant::now();
    let payload = store.get(&path).await?;
    println!("get in {:?}", start.elapsed());
    println!("returned payload={:?}", payload);

    let start = Instant::now();
    let _ = store.get(&Path::parse(".testing/missing.txt")?).await?;
    println!("get missing in {:?}", start.elapsed());

    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(e) = test_test().await {
        eprintln!("Error: {}", e);
    }
    if let Err(e) = object_store_test().await {
        eprintln!("Error: {}", e);
    }
}
