use lru::LruCache;
use std::num::NonZeroUsize;
use tungstenite::connect;

mod post_buffer;
use post_buffer::PostBuffer;
mod post_handler;
mod schemas;

#[tokio::main]
async fn main() {
    let post_map: LruCache<String, String> = LruCache::new(NonZeroUsize::new(8000000).unwrap());

    let post_buffer = PostBuffer::new(
        schemas::post_schema::create_post_schema(),
        schemas::embedding_schema::create_embedding_schema(),
        4000,
    );
    schemas::post_schema::create_post_schema();
    let ws_url =
        "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post";
    let (mut socket, _response) = connect(ws_url).expect("Failed to connect");
    let mut post_handler = post_handler::PostHandler::new(post_map, post_buffer);

    loop {
        match socket.read() {
            Ok(msg) => {
                if msg.is_text() || msg.is_binary() {
                    let text = msg
                        .clone()
                        .into_text()
                        .expect("Failed to convert message to text");
                    if let Err(err) = post_handler.handle_post(&text).await {
                        eprintln!("Error handling post: {}", err);
                    }
                }
            }
            Err(err) => {
                eprintln!("Error reading message: {}", err);
                break;
            }
        }
    }
}
