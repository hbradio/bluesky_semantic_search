# Bluesky Semantic Search

These modules ingest Bluesky posts, performed text embedding, and builds a vector db for semantic search.

## Highlights

* Uses sentences-transformers to do AllMiniLML6v2 text embeddings
* Stores data in Arrow + Parquet
* Builds a Lance vector db with an IVF_FLAT index.
* Hardware-accelerated on Apple Metal

## Notes

I am a Rust beginner. To write this code, I used regular ol' docs as well as Claude Code in planning mode. Claude created examples and helped me debug borrow-checker errors, but I typed the code.


