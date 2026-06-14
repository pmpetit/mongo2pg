use std::collections::{HashMap, HashSet};

use crate::schema_diagram::Table;

/// Build mapping mongo_path from FK ancestry.
///
/// Root table => "."
/// Child table => ".<root>.<parent>..." (ancestors only, no current table)
pub fn mapping_mongo_path_for_table(
    table_name: &str,
    tables_by_name: &HashMap<String, Table>,
) -> Option<String> {
    let mut ancestors = Vec::new();
    let mut current = table_name.to_owned();
    let mut visited = HashSet::new();

    while let Some(parent) = tables_by_name
        .get(&current)
        .and_then(|table| table.foreign_keys.first())
        .map(|fk| fk.to_table.clone())
    {
        if !visited.insert(parent.clone()) {
            break;
        }
        ancestors.push(parent.clone());
        current = parent;
    }

    if ancestors.is_empty() {
        Some(".".to_owned())
    } else {
        ancestors.reverse();
        Some(format!(".{}", ancestors.join(".")))
    }
}
