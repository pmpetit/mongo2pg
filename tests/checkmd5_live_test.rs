use std::path::PathBuf;

use mongo2pg::checkmd5::compute_md5_summaries_for_collection;

fn sample_training_config_path() -> PathBuf {
    std::env::var("MONGO2PG_SAMPLE_TRAINING_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("results/sample_training/config/sample_training.toml"))
}

#[tokio::test]
#[ignore = "requires running MongoDB/PostgreSQL containers and populated sample_training dataset"]
async fn checkmd5_sample_training_companies_live() {
    let config_path = sample_training_config_path();
    assert!(
        config_path.exists(),
        "config not found at {}",
        config_path.display()
    );

    let summaries = compute_md5_summaries_for_collection("companies", &config_path)
        .await
        .unwrap_or_else(|err| {
            panic!(
                "checkmd5 failed for companies with config {}: {err}",
                config_path.display()
            )
        });

    assert!(
        !summaries.is_empty(),
        "no checkmd5 summaries produced for companies"
    );

    let companies = summaries
        .iter()
        .find(|summary| summary.table_name == "companies")
        .unwrap_or_else(|| {
            panic!(
                "root table summary 'companies' not found; got tables: {:?}",
                summaries
                    .iter()
                    .map(|summary| summary.table_name.clone())
                    .collect::<Vec<_>>()
            )
        });

    assert!(
        !companies.summary.columns.is_empty(),
        "companies summary has no mapped columns"
    );
    assert_eq!(
        companies.summary.mongo_md5.len(),
        32,
        "mongo md5 must be a 32-char hex digest"
    );
    assert_eq!(
        companies.summary.pg_md5.len(),
        32,
        "pg md5 must be a 32-char hex digest"
    );
}
