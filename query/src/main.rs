use arrow::array::RecordBatch;
use glob::glob;
use std::fs::File;

use arrow::array::Float32Array;
use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatchIterator;
use datafusion::prelude::*;
use lance::Dataset;
use lance::index::vector::VectorIndexParams;
use lance_index::{DatasetIndexExt, IndexType};
use lance_linalg::distance::DistanceType;
use log;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sentence_transformers_rs::sentence_transformer::{SentenceTransformerBuilder, Which};

#[tokio::main]
async fn main() {
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .init();
    let embeddings_paths = glob("/Users/bhurlburt/Code/Mine/conversation-arcs/datalake/embeddings/year/2025/month/10/day/20/hour/*/*.parquet").unwrap().filter_map(Result::ok);
    let post_paths = glob("/Users/bhurlburt/Code/Mine/conversation-arcs/datalake/posts/year/2025/month/10/day/20/hour/*/*.parquet").unwrap().filter_map(Result::ok);

    log::info!("Embedding query...");

    let device = candle_core::Device::new_metal(0).unwrap();
    let sentence_transformer =
        SentenceTransformerBuilder::with_sentence_transformer(&Which::AllMiniLML6v2)
            .batch_size(512)
            .with_device(&device)
            .build()
            .unwrap();

    let sentences = vec!["Today is a nice day"];
    let embeddings = sentence_transformer.embed(&sentences).unwrap();

    log::info!("> Query embedded.");

    log::info!("Reading embeddings files...");
    let all_embeddings: Vec<RecordBatch> = embeddings_paths
        .flat_map(|path| {
            let file = File::open(path).unwrap();
            let reader = ParquetRecordBatchReaderBuilder::try_new(file)
                .unwrap()
                .build()
                .unwrap();
            reader.collect::<Result<Vec<_>, _>>().unwrap()
        })
        .collect();

    let embeddings_schema = all_embeddings[0].schema();
    let embeddings_combined_batch = concat_batches(&embeddings_schema, &all_embeddings).unwrap();
    let embeddings_reader =
        RecordBatchIterator::new(all_embeddings.into_iter().map(Ok), embeddings_schema);

    log::info!(
        "> Read {} total embeddings records.",
        embeddings_combined_batch.num_rows()
    );

    log::info!("Reading posts files...");
    let all_posts: Vec<RecordBatch> = post_paths
        .flat_map(|path| {
            let file = File::open(path).unwrap();
            let reader = ParquetRecordBatchReaderBuilder::try_new(file)
                .unwrap()
                .build()
                .unwrap();
            reader.collect::<Result<Vec<_>, _>>().unwrap()
        })
        .collect();

    let posts_schema = all_posts[0].schema();
    let posts_combined_batch = concat_batches(&posts_schema, &all_posts).unwrap();

    log::info!(
        "> Read {} total post records.",
        posts_combined_batch.num_rows()
    );
    log::info!("Building vector database...");

    let uri = format!("file:///tmp/vectors_{}.lance", uuid::Uuid::new_v4());
    let mut dataset = Dataset::write(embeddings_reader, &uri, None).await.unwrap();

    let params = VectorIndexParams::ivf_flat(64, DistanceType::Cosine);

    dataset
        .create_index(&["embedding"], IndexType::Vector, None, &params, false)
        .await
        .unwrap();

    log::info!("> Vector database built.");
    log::info!("Running query...");

    let query_array = Float32Array::from(embeddings[0].clone());
    let result_record_batch = dataset
        .scan()
        .nearest("embedding", &query_array as &dyn arrow::array::Array, 10)
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();

    log::info!("Query completed.");

    log::info!("Retrieving posts...");
    let ctx = SessionContext::new();
    ctx.register_batch("results", result_record_batch).unwrap();
    ctx.register_batch("posts", posts_combined_batch).unwrap();
    let df = ctx
        .sql("SELECT results.id, posts.text FROM results JOIN posts ON results.id = posts.id")
        .await
        .unwrap();
    df.show().await.unwrap();
}
