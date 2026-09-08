use bytes::Bytes;
use futures::SinkExt;
use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};
use tokio_postgres::NoTls;

fn sanitize_dlq_collection_component(collection_name: &str) -> String {
    let cleaned = collection_name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    cleaned.trim_matches('_').to_owned()
}

fn adhoc_dlq_topic_for_collection(collection_name: &str) -> String {
    let component = sanitize_dlq_collection_component(collection_name);
    if component.is_empty() {
        "dlq_unknown_collection".to_owned()
    } else {
        format!("dlq_{component}")
    }
}

async fn start_pg_client() -> anyhow::Result<tokio_postgres::Client> {
    let pg_container = postgres::Postgres::default().start().await?;
    let pg_port = pg_container.get_host_port_ipv4(5432).await?;
    let pg_conn_str = format!(
        "postgres://postgres:postgres@localhost:{pg_port}/postgres?sslmode=disable"
    );
    let (pg_client, pg_connection) = tokio_postgres::connect(&pg_conn_str, NoTls).await?;
    tokio::spawn(async move {
        let _ = pg_connection.await;
    });

    // Keep container alive for test lifetime.
    std::mem::forget(pg_container);
    Ok(pg_client)
}

async fn copy_batch_transactional(
    pg_client: &mut tokio_postgres::Client,
    table_name: &str,
    payload_csv_rows: &str,
) -> anyhow::Result<u64> {
    let copy_sql = format!(
        "COPY {} (id, name) FROM STDIN WITH (FORMAT csv, NULL '\\\\N')",
        table_name
    );

    let transaction = pg_client.transaction().await?;
    let sink = transaction.copy_in(&copy_sql).await?;
    let mut sink = std::pin::pin!(sink);
    sink.as_mut()
        .send(Bytes::copy_from_slice(payload_csv_rows.as_bytes()))
        .await?;
    let rows = match sink.as_mut().finish().await {
        Ok(rows) => rows,
        Err(err) => {
            let _ = transaction.rollback().await;
            return Err(anyhow::anyhow!(err));
        }
    };
    transaction.commit().await?;
    Ok(rows)
}

#[tokio::test]
#[ignore = "requires docker testcontainers"]
async fn snapshot_copy_batch_size_controls_flush_shape_and_is_transactional() {
    let mut pg_client = start_pg_client().await.expect("postgres should start");

    pg_client
        .batch_execute(
            "
            DROP TABLE IF EXISTS public.kafka_copy_users;
            CREATE TABLE public.kafka_copy_users (
              id INTEGER PRIMARY KEY,
              name TEXT NOT NULL
            );
            ",
        )
        .await
        .expect("table create should succeed");

    // batch_size=1 equivalent: each row flushed independently.
    copy_batch_transactional(&mut pg_client, "public.kafka_copy_users", "1,Alice\n")
        .await
        .expect("copy row 1 should succeed");
    copy_batch_transactional(&mut pg_client, "public.kafka_copy_users", "2,Bob\n")
        .await
        .expect("copy row 2 should succeed");

    let row = pg_client
        .query_one("SELECT COUNT(*)::INT FROM public.kafka_copy_users", &[])
        .await
        .expect("count should succeed");
    let count_after_batch_1: i32 = row.get(0);
    assert_eq!(count_after_batch_1, 2);

    pg_client
        .batch_execute("TRUNCATE TABLE public.kafka_copy_users")
        .await
        .expect("truncate should succeed");

    // batch_size=2 equivalent: two rows flushed in one COPY payload.
    copy_batch_transactional(
        &mut pg_client,
        "public.kafka_copy_users",
        "10,Carol\n11,Dan\n",
    )
    .await
    .expect("copy 2-row batch should succeed");

    let row = pg_client
        .query_one("SELECT COUNT(*)::INT FROM public.kafka_copy_users", &[])
        .await
        .expect("count should succeed");
    let count_after_batch_2: i32 = row.get(0);
    assert_eq!(count_after_batch_2, 2);
}

#[tokio::test]
#[ignore = "requires docker testcontainers"]
async fn snapshot_copy_failure_rolls_back_and_fallback_replay_routes_bad_rows_to_adhoc_dlq() {
    let mut pg_client = start_pg_client().await.expect("postgres should start");

    pg_client
        .batch_execute(
            "
            DROP TABLE IF EXISTS public.kafka_copy_replay;
            CREATE TABLE public.kafka_copy_replay (
              id INTEGER PRIMARY KEY,
              name TEXT NOT NULL
            );
            ",
        )
        .await
        .expect("table create should succeed");

    // One invalid row (bad integer) should fail full COPY batch and rollback.
    let copy_result = copy_batch_transactional(
        &mut pg_client,
        "public.kafka_copy_replay",
        "100,Good\nbad,BadType\n",
    )
    .await;
    assert!(copy_result.is_err(), "copy should fail for invalid integer");

    let row = pg_client
        .query_one("SELECT COUNT(*)::INT FROM public.kafka_copy_replay", &[])
        .await
        .expect("count should succeed");
    let count_after_failed_copy: i32 = row.get(0);
    assert_eq!(count_after_failed_copy, 0, "failed COPY batch must rollback");

    // Fallback single-row replay simulation.
    let mut adhoc_dlq_payloads: Vec<(String, String)> = Vec::new();
    let fallback_rows = vec!["100,Good", "bad,BadType"];
    for raw in fallback_rows {
        let parts = raw.splitn(2, ',').collect::<Vec<_>>();
        let topic = adhoc_dlq_topic_for_collection("users");
        if parts.len() != 2 {
            adhoc_dlq_payloads.push((topic, raw.to_owned()));
            continue;
        }

        let id = parts[0].parse::<i32>();
        let name = parts[1].to_owned();
        match id {
            Ok(parsed_id) => {
                let inserted = pg_client
                    .execute(
                        "INSERT INTO public.kafka_copy_replay(id, name) VALUES($1, $2)",
                        &[&parsed_id, &name],
                    )
                    .await;
                if inserted.is_err() {
                    adhoc_dlq_payloads.push((topic, raw.to_owned()));
                }
            }
            Err(_) => {
                adhoc_dlq_payloads.push((topic, raw.to_owned()));
            }
        }
    }

    let row = pg_client
        .query_one("SELECT COUNT(*)::INT FROM public.kafka_copy_replay", &[])
        .await
        .expect("count should succeed");
    let count_after_fallback: i32 = row.get(0);
    assert_eq!(count_after_fallback, 1, "one good row should replay successfully");

    assert_eq!(adhoc_dlq_payloads.len(), 1, "one fallback row should reach DLQ");
    assert_eq!(adhoc_dlq_payloads[0].0, "dlq_users");
    assert_eq!(adhoc_dlq_payloads[0].1, "bad,BadType");
}
