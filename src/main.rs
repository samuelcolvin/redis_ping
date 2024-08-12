use std::env;
use std::time::Instant;
use anyhow::Context;
use redis::AsyncCommands;

async fn get_redis_value() -> anyhow::Result<()> {
    let connection_params = env::var("REDIS_DSN").context("REDIS_DSN not set")?;
    let client = redis::Client::open(connection_params)?;
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

#[tokio::main]
async fn main() {
    if let Err(e) = get_redis_value().await {
        eprintln!("Error: {}", e);
    }
}
