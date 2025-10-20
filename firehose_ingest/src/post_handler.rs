use lru::LruCache;
use serde::Deserialize;
use uuid::Uuid;

use crate::post_buffer::PostBuffer;

#[derive(Deserialize, Debug)]
struct Post {
    commit: Commit,
}

#[derive(Deserialize, Debug)]
struct Commit {
    record: Record,
    cid: String,
}

#[derive(Deserialize, Debug)]
struct Record {
    #[serde(rename = "createdAt")]
    created_at: String,
    text: String,
    reply: Option<Reply>,
    #[serde(default)]
    langs: Vec<String>,
}

#[derive(Deserialize, Debug)]
struct Reply {
    root: Root,
}

#[derive(Deserialize, Debug)]
struct Root {
    cid: String,
}

pub struct PostHandler {
    post_map: LruCache<String, String>,
    post_buffer: PostBuffer,
}

impl PostHandler {
    pub fn new(post_map: LruCache<String, String>, post_buffer: PostBuffer) -> Self {
        PostHandler {
            post_map,
            post_buffer,
        }
    }

    pub async fn handle_post(&mut self, text: &str) -> Result<(), String> {
        match serde_json::from_str::<Post>(&text) {
            Ok(deserialized) => {
                // println!("{:?}", deserialized);
                let text = deserialized.commit.record.text;
                let cid = deserialized.commit.cid;
                let parent_cid = deserialized
                    .commit
                    .record
                    .reply
                    .map(|r| r.root.cid)
                    .unwrap_or_else(|| String::new());
                if deserialized
                    .commit
                    .record
                    .langs
                    .iter()
                    .any(|lang| lang == "en")
                {
                    let new_id = Uuid::new_v4().to_string();
                    let mut full_text = text.clone();
                    match self.post_map.get(&parent_cid) {
                        Some(parent_text) => {
                            full_text.push_str(format!(" {}", parent_text).as_str());
                        }
                        None => (),
                    }
                    match self
                        .post_buffer
                        .add(
                            new_id,
                            &deserialized.commit.record.created_at,
                            full_text.clone(),
                        )
                        .await
                    {
                        Ok(_) => (),
                        Err(err) => println!("Failed to add post to buffer: {}", err),
                    }
                    self.post_map.put(cid, text);
                    Ok(())
                } else {
                    Ok(())
                }
            }
            Err(err) => {
                if err.to_string().contains("missing field") {
                    Ok(())
                } else {
                    Err(err.to_string())
                }
            }
        }
    }
}
