/// Build mapping mongo_path from MongoDB collection + field-path segments.
///
/// Root mapping => "."
/// Child mapping => ".<segment>..."
pub fn mapping_mongo_path_for_segments(
    _root_collection_name: &str,
    mongo_path_segments: &[String],
) -> Option<String> {
    if mongo_path_segments.is_empty() {
        Some(".".to_owned())
    } else {
        Some(format!(".{}", mongo_path_segments.join(".")))
    }
}
