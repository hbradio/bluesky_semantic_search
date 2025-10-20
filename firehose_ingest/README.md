# Bluesky Firehose Ingest

Subscribes to the [Bluesky JSON firehose](https://docs.bsky.app/docs/advanced-guides/firehose).
Uses [sentence-transformers-rs](https://github.com/jwnz/sentence-transformers-rs) to perform AllMiniLML6v2 text embeddings.
Saves posts and embeddings to Parquet files.
