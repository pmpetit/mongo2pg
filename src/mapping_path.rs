/// Build mapping mongo_path from MongoDB collection + field-path segments.
///
/// Root mapping => "."
/// Child mapping => ".<collection>.<segment>..."
pub fn mapping_mongo_path_for_segments(
    root_collection_name: &str,
    mongo_path_segments: &[String],
) -> Option<String> {
    if mongo_path_segments.is_empty() {
        Some(".".to_owned())
    } else {
        Some(format!(
            ".{}.{}",
            root_collection_name,
            mongo_path_segments.join(".")
        ))
    }
}
