use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

pub fn create_post_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("text", DataType::Utf8, false),
    ]))
}
