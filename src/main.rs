#![allow(dead_code, unused_imports)]
use std::env;
use std::hint::black_box;
use std::process::exit;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use arrow::array::{LargeStringBuilder, RecordBatch, UInt64Builder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use bytes::{BufMut, Bytes, BytesMut};
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use redis::aio::MultiplexedConnection;
use redis::AsyncCommands;
use rand::rngs::ThreadRng;
use rand::{thread_rng, Rng};

#[tokio::main]
async fn main() {
    // if let Err(e) = test_test().await {
    //     eprintln!("Error: {}", e);
    //     exit(1);
    // }
    if let Err(e) = build_parquet().await {
        eprintln!("Error: {}", e);
        exit(2);
    }
    // if let Err(e) = object_store_test().await {
    //     eprintln!("Error: {}", e);
    //     exit(2);
    // }
}

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
    println!(
        "REDIS get {:?} in {:?} value={:?}",
        key,
        start.elapsed(),
        value
    );

    let start = Instant::now();
    let value: Option<String> = con.get("missing").await?;
    println!(
        "REDIS get 'missing' in {:?} value={:?}",
        start.elapsed(),
        value
    );

    let key = "big_chunk";
    let start = Instant::now();
    con.set(key, big_chunk()).await?;
    println!("REDIS set '{}' in {:?}", key, start.elapsed());

    let start = Instant::now();
    let value: Option<Vec<u8>> = con.get(key).await?;
    println!(
        "REDIS get '{key}' in {:?} value.is_some() {:?}",
        start.elapsed(),
        value.is_some()
    );
    black_box(value);

    Ok(())
}

async fn object_store_test() -> anyhow::Result<()> {
    let gcs_dsn = env::var("GCS_DSN").context("GCS_DSN not set")?;
    let store = GoogleCloudStorageBuilder::from_env()
        .with_url(gcs_dsn)
        .build()?;
    let path = Path::parse(".testing/file.txt")?;
    let payload = PutPayload::from_static(b"this is a test");
    let start = Instant::now();
    store.put(&path, payload).await?;
    println!("GCS put in {:?}", start.elapsed());

    let start = Instant::now();
    let payload = store.get(&path).await?;
    println!("GCS get in {:?}", start.elapsed());
    black_box(payload);

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

const PARQUET_KEY: &str = "testing-parquet";

async fn build_parquet() -> anyhow::Result<()> {
    let redis_dsn = env::var("REDIS_DSN").context("REDIS_DSN not set")?;
    let client = redis::Client::open(redis_dsn)?;
    let mut con = client.get_multiplexed_async_connection().await?;

    con.del(PARQUET_KEY).await?;
    let mut rng = thread_rng();

    let write_count = 30;

    let batches = (0..write_count)
        .map(|_| create_batch(&mut rng))
        .collect::<Result<Vec<_>, _>>()?;
    let start = Instant::now();
    let mut size = 0;
    for batch in batches {
        size = write_parquet_to_redis(batch, &mut con).await?;
    }
    println!(
        "write_parquet_to_redis {} times in {:?} final size {}",
        write_count,
        start.elapsed(),
        SizePretty(size as u64)
    );
    let last_batch = create_batch(&mut rng)?;
    let start = Instant::now();
    let size = write_parquet_to_redis(last_batch, &mut con).await?;
    println!(
        "write_parquet_to_redis last batch in {:?} final size {}",
        start.elapsed(),
        SizePretty(size as u64)
    );
    con.del(PARQUET_KEY).await.map_err(Into::into)
}

/// WARNING: this NEEDS locking to be safe
async fn write_parquet_to_redis(
    batch: RecordBatch,
    con: &mut MultiplexedConnection,
) -> anyhow::Result<usize> {
    let schema = batch.schema();
    let existing_data: Option<Vec<u8>> = con.get(PARQUET_KEY).await?;
    let batches = if let Some(data) = existing_data {
        let bytes = Bytes::from(data);
        let parquet_reader = ParquetRecordBatchReader::try_new(bytes, 8192)?;
        let mut batches: Vec<RecordBatch> = parquet_reader
            .map(|b| b)
            .collect::<Result<_, ArrowError>>()?;
        batches.push(batch);
        batches
    } else {
        vec![batch]
    };

    let writer_props = parquet_writer_props();
    let bytes = BytesMut::with_capacity(8 * 1024 * 1024);
    let mut writer = bytes.writer();
    {
        let mut arrow_writer =
            ArrowWriter::try_new(&mut writer, schema, Some(writer_props))?;
        for batch in batches {
            arrow_writer.write(&batch)?;
        }
        arrow_writer.finish()?;
    }

    let bytes = writer.into_inner().freeze();
    let bytes = bytes.to_vec();
    let size = bytes.len();

    con.set(PARQUET_KEY, bytes).await?;
    Ok(size)
}

const ROWS: usize = 10_000;

fn create_batch(rng: &mut ThreadRng) -> Result<RecordBatch, ArrowError> {
    let mut int_builder = UInt64Builder::with_capacity(ROWS);
    let mut str_builder = LargeStringBuilder::with_capacity(ROWS, ROWS * 100);

    for row in 0..ROWS {
        int_builder.append_value(row as u64);
        let s = random_string(rng);
        str_builder.append_value(s);
    }

    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("string", DataType::LargeUtf8, true),
        ])),
        vec![
            Arc::new(int_builder.finish()),
            Arc::new(str_builder.finish()),
        ],
    )
}


fn random_string(rng: &mut ThreadRng) -> String {
    (0..rng.gen_range(1..=50))
        .map(|_| rng.sample(rand::distributions::Alphanumeric) as char)
        .collect()
}


pub fn parquet_writer_props() -> WriterProperties {
    // let compression = Compression::ZSTD(ZstdLevel::try_new(3).unwrap());
    let compression = Compression::SNAPPY;
    WriterProperties::builder()
        .set_compression(compression)
        // dictionary encoding is on by default but make it explicit here
        .set_dictionary_enabled(true)
        .build()
}


pub struct SizePretty(pub u64);

impl std::fmt::Display for SizePretty {
    #[allow(clippy::cast_precision_loss)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0 < 1024 {
            write!(f, "{} B", self.0)
        } else if self.0 < 1024 * 1024 {
            write!(f, "{:.2} KB", self.0 as f64 / 1024.0)
        } else {
            write!(f, "{:.2} MB", self.0 as f64 / 1024.0 / 1024.0 / 1.0)
        }
    }
}
