//! `cluster-report` subcommand handler.

use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use log::info;

use crate::cli::ClusterReportArgs;
use crate::report::{cluster_from_uri, collect_rows, compute_db_score, render_cluster_html};
use crate::util::{configured_project_root, read_conf};

pub fn run_cluster_report(args: ClusterReportArgs) -> Result<()> {
    if args.configs.is_empty() {
        return Err(anyhow!("Provide at least one config path via --configs"));
    }

    let mut db_scores = Vec::new();
    let mut cluster_label = args.cluster_label.clone();

    for conf_path in &args.configs {
        let c = read_conf(conf_path)?;

        // Derive the database label: NAMESPACE from config, or fall back to PROJECT_DIR.
        let db_name = c.namespace.clone().unwrap_or_else(|| c.project_dir.clone());

        // Derive the cluster label from the first config that has a URI.
        if cluster_label.is_empty() {
            if let Some(ref source_uri) = c.source_uri {
                cluster_label = cluster_from_uri(source_uri);
            }
        }

        let collections_dir = configured_project_root(&c)
            .join("source")
            .join("collections");

        let rows = collect_rows(&collections_dir, None)
            .with_context(|| format!("Failed to read collections for {db_name}"))?;

        db_scores.push(compute_db_score(&db_name, &rows));
    }

    let html = render_cluster_html(&db_scores, &cluster_label);

    let output_path = args.output.unwrap_or_else(|| PathBuf::from("cluster.html"));
    std::fs::write(&output_path, &html)
        .with_context(|| format!("Failed to write {}", output_path.display()))?;
    info!("Cluster report written to {}", output_path.display());

    Ok(())
}
