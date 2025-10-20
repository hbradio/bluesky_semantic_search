use arrow::array::{
    ArrayRef, FixedSizeListArray, Float32Array, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Datelike, Timelike, Utc};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use sentence_transformers_rs::sentence_transformer::{
    SentenceTransformer, SentenceTransformerBuilder, Which,
};
use std::fs::{OpenOptions, create_dir_all};
use std::sync::Arc;
use tokio;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::schemas::embedding_schema;

pub struct PostBuffer {
    ids: Vec<String>,
    timestamps: Vec<i64>,
    texts: Vec<String>,
    max_size: usize,
    post_schema: Arc<Schema>,
    embeddings_schema: Arc<Schema>,
    model: Arc<Mutex<SentenceTransformer>>,
}

impl PostBuffer {
    pub fn new(post_schema: Arc<Schema>, embeddings_schema: Arc<Schema>, max_size: usize) -> Self {
        let device = candle_core::Device::new_metal(0).unwrap();
        PostBuffer {
            ids: Vec::with_capacity(max_size),
            timestamps: Vec::with_capacity(max_size),
            texts: Vec::with_capacity(max_size),
            max_size,
            post_schema,
            embeddings_schema,
            model: Arc::new(Mutex::new(
                SentenceTransformerBuilder::with_sentence_transformer(&Which::AllMiniLML6v2)
                    .batch_size(4096)
                    .with_device(&device)
                    .build()
                    .unwrap(),
            )),
        }
    }

    pub async fn add(
        &mut self,
        id: String,
        created_at: &str,
        text: String,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        let dt = DateTime::parse_from_rfc3339(created_at).unwrap();
        let timestamp_micros = dt.timestamp_micros();

        self.ids.push(id);
        self.timestamps.push(timestamp_micros);
        self.texts.push(text);

        if self.timestamps.len() >= self.max_size {
            tokio::spawn(Self::flush(
                self.ids.clone(),
                self.timestamps.clone(),
                self.texts.clone(),
                self.post_schema.clone(),
                self.embeddings_schema.clone(),
                self.model.clone(),
            ));
            self.ids.clear();
            self.timestamps.clear();
            self.texts.clear();
        };
        Ok(())
    }

    async fn flush(
        ids: Vec<String>,
        timestamps: Vec<i64>,
        texts: Vec<String>,
        post_schema: Arc<Schema>,
        embeddings_schema: Arc<Schema>,
        model: Arc<Mutex<SentenceTransformer>>,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        if timestamps.is_empty() {
            return Ok(());
        }

        let text_refs: Vec<&str> = texts.iter().map(|s| s.as_str()).collect();
        let start_time = Utc::now();
        println!("Start Time: {}", start_time);
        let embeddings_result = {
            let model = model.lock().await;
            model.embed(&text_refs)
        };
        println!(
            "Elapsed Time: {}",
            Utc::now().signed_duration_since(start_time)
        );
        let embeddings = embeddings_result.unwrap();
        println!("Embeddings: {}", embeddings.len());

        let post_record_batch = RecordBatch::try_new(
            post_schema.clone(),
            vec![
                Arc::new(StringArray::from(ids.clone())) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(timestamps.clone()).with_timezone("UTC"))
                    as ArrayRef,
                Arc::new(StringArray::from(texts.clone())) as ArrayRef,
            ],
        )
        .unwrap();

        let flat_embeddings: Vec<f32> = embeddings.iter().flatten().copied().collect();
        let embeddings_values = Arc::new(Float32Array::from(flat_embeddings));
        let embeddings_record_batch = RecordBatch::try_new(
            embeddings_schema.clone(),
            vec![
                Arc::new(StringArray::from(ids.clone())) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(timestamps.clone()).with_timezone("UTC"))
                    as ArrayRef,
                Arc::new(
                    FixedSizeListArray::try_new(
                        Arc::new(Field::new("item", DataType::Float32, false)),
                        384,
                        embeddings_values,
                        None,
                    )
                    .unwrap(),
                ) as ArrayRef,
            ],
        )
        .unwrap();

        let current_hour = Utc::now();

        let posts_path = format!(
            "/Users/bhurlburt/Code/Mine/conversation-arcs/datalake/posts/year/{}/month/{}/day/{}/hour/{}/",
            current_hour.year(),
            current_hour.month(),
            current_hour.day(),
            current_hour.hour()
        );
        let embeddings_path = format!(
            "/Users/bhurlburt/Code/Mine/conversation-arcs/datalake/embeddings/year/{}/month/{}/day/{}/hour/{}/",
            current_hour.year(),
            current_hour.month(),
            current_hour.day(),
            current_hour.hour()
        );
        create_dir_all(&posts_path).unwrap();
        create_dir_all(&embeddings_path).unwrap();
        let posts_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(format!("{}/data_{}.parquet", &posts_path, Uuid::new_v4()))
            .unwrap();
        let embeddings_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(format!(
                "{}/data_{}.parquet",
                &embeddings_path,
                Uuid::new_v4()
            ))
            .unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut posts_writer =
            ArrowWriter::try_new(posts_file, post_schema.clone(), Some(props.clone())).unwrap();
        posts_writer
            .write(&post_record_batch)
            .expect("Writing posts batch");
        posts_writer.close().unwrap();
        let mut embeddings_writer =
            ArrowWriter::try_new(embeddings_file, embeddings_schema.clone(), Some(props)).unwrap();
        embeddings_writer
            .write(&embeddings_record_batch)
            .expect("Writing embeddings batch");
        embeddings_writer.close().unwrap();

        println!("Wrote batches at {}\n\n", Utc::now());

        Ok(())
    }
}
