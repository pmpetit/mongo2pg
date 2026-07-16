#[cfg(test)]
#[allow(unused)]
pub struct TestHarness {
    _pg_container: Box<dyn std::any::Any>,
    _mongo_container: Box<dyn std::any::Any>,
    pub pg_client: tokio_postgres::Client,
    pub pg_read_client: tokio_postgres::Client,
    pub mongo_collection: mongodb::Collection<bson::Document>,
    pub schema_name: Option<String>,
    pub table_name: String,
    pub document: bson::Document,
}

#[cfg(test)]
impl TestHarness {
    pub async fn new(
        json_fixture: &str,
        sql_fixture: &str,
        yaml_fixture: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        use super::super::read_mapping_yaml;
        use std::fs::{self, File};
        use std::io::BufReader;
        use std::path::PathBuf;
        use testcontainers_modules::{mongo, postgres, testcontainers::runners::AsyncRunner};
        use tokio_postgres::types::ToSql;
        use tokio_postgres::NoTls;

        // 1. Core infrastructure spun up in parallel
        let (pg_container, mongo_container) = tokio::join!(
            postgres::Postgres::default().start(),
            mongo::Mongo::default().start()
        );
        let pg_container = pg_container?;
        let mongo_container = mongo_container?;

        // 2. Setup MongoDB Collection
        let mongo_port = mongo_container.get_host_port_ipv4(27017).await?;
        let mongo_client =
            mongodb::Client::with_uri_str(&format!("mongodb://localhost:{mongo_port}")).await?;
        let mongo_collection = mongo_client
            .database("test_db")
            .collection::<bson::Document>("employees");

        // 3. Setup PostgreSQL Connection
        let pg_port = pg_container.get_host_port_ipv4(5432).await?;
        let pg_conn_str =
            format!("postgres://postgres:postgres@localhost:{pg_port}/postgres?sslmode=disable");
        let (pg_client, pg_connection) = tokio_postgres::connect(&pg_conn_str, NoTls).await?;
        let (pg_read_client, pg_read_connection) =
            tokio_postgres::connect(&pg_conn_str, NoTls).await?;

        tokio::spawn(async move {
            if let Err(err) = pg_connection.await {
                eprintln!("PostgreSQL connection error: {err}");
            }
        });
        tokio::spawn(async move {
            if let Err(err) = pg_read_connection.await {
                eprintln!("PostgreSQL read connection error: {err}");
            }
        });

        // 4. Extract values from config mappings
        let mapping_yaml = read_mapping_yaml(&PathBuf::from(yaml_fixture))
            .expect("Failed to parse schema alignments YAML");
        let schema_name = mapping_yaml.pg_mapping.schema_name.clone();
        let table_name = mapping_yaml.pg_mapping.table_name.clone();

        // 5. Read JSON fixture document base
        let file = File::open(json_fixture)?;
        let json_val: serde_json::Value = serde_json::from_reader(BufReader::new(file))?;
        let base_document: bson::Document = bson::to_document(&json_val)?;

        // 6. Execute DDL Schema creation script
        if let Some(ref schema) = schema_name {
            pg_client
                .execute(&format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\";"), &[])
                .await?;
        }
        let qualified_table = match schema_name {
            Some(ref schema) => format!("\"{schema}\".\"{table_name}\""),
            None => format!("\"{table_name}\""),
        };
        let ddl_sql = fs::read_to_string(sql_fixture)?.replace(
            "CREATE TABLE listingsandreviews",
            &format!("CREATE TABLE {qualified_table}"),
        );
        pg_client.batch_execute(&ddl_sql).await?;

        // Target insertion matrix mapped exactly to your parameters sequence
        let insert_sql = format!(
            "INSERT INTO {qualified_table} (
                id, access, accommodates, amenities, bathrooms, bed_type, bedrooms, beds, calendar_last_scraped,
                cancellation_policy, cleaning_fee, description, extra_people, first_review, guests_included,
                house_rules, interaction, last_review, last_scraped, listing_url, maximum_nights, minimum_nights,
                monthly_price, name, neighborhood_overview, notes, number_of_reviews, price, property_type,
                reviews_per_month, room_type, security_deposit, space, summary, transit, weekly_price
            ) VALUES (
                $1, $2, $3, $4, $5::double precision, $6, $7, $8, $9, $10, $11::double precision, $12, $13::double precision,
                $14, $15::double precision, $16, $17, $18, $19, $20, $21, $22, $23::double precision, $24, $25, $26,
                $27, $28::double precision, $29, $30, $31, $32::double precision, $33, $34, $35, $36::double precision
            );"
        );

        // Get the base ID from the fixture to increment off of
        let base_id: i64 = base_document.get_str("_id")?.parse()?;

        // Loop 20 times to insert 20 distinct items
        for i in 0..20 {
            let mut document = base_document.clone();
            let id_val = base_id + i;

            // Update the document ID to ensure uniqueness in MongoDB
            document.insert("_id", id_val.to_string());

            // Save into MongoDB
            mongo_collection.insert_one(document.clone()).await?;

            // 7. Dynamic SQL binding mapper
            let extract_str = |key: &str| document.get_str(key).unwrap_or("").to_string();
            let extract_int = |key: &str| -> i32 {
                match document.get(key) {
                    Some(bson::Bson::Int32(v)) => *v,
                    Some(bson::Bson::Int64(v)) => *v as i32,
                    _ => 0,
                }
            };
            let extract_opt_int = |key: &str| -> Option<i32> {
                match document.get(key) {
                    Some(bson::Bson::Int32(v)) => Some(*v),
                    Some(bson::Bson::Int64(v)) => Some(*v as i32),
                    _ => None,
                }
            };
            let extract_f64 = |key: &str| document.get_f64(key).ok();
            let extract_ts = |key: &str| -> chrono::DateTime<chrono::Utc> {
                chrono::DateTime::parse_from_rfc3339(&extract_str(key))
                    .unwrap()
                    .with_timezone(&chrono::Utc)
            };
            let extract_opt_ts = |key: &str| -> Option<chrono::DateTime<chrono::Utc>> {
                document.get_str(key).ok().map(|val| {
                    chrono::DateTime::parse_from_rfc3339(val)
                        .unwrap()
                        .with_timezone(&chrono::Utc)
                })
            };

            let amenities: Vec<String> = document
                .get_array("amenities")
                .unwrap_or(&vec![])
                .iter()
                .filter_map(|b| b.as_str().map(|s| s.to_string()))
                .collect();

            let params: Vec<Box<dyn ToSql + Sync>> = vec![
                Box::new(id_val),
                Box::new(extract_str("access")),
                Box::new(extract_int("accommodates")),
                Box::new(amenities),
                Box::new(extract_f64("bathrooms")),
                Box::new(extract_str("bed_type")),
                Box::new(extract_opt_int("bedrooms")),
                Box::new(extract_opt_int("beds")),
                Box::new(extract_ts("calendar_last_scraped")),
                Box::new(extract_str("cancellation_policy")),
                Box::new(extract_f64("cleaning_fee")),
                Box::new(extract_str("description")),
                Box::new(extract_f64("extra_people").unwrap_or(0.0)),
                Box::new(extract_opt_ts("first_review")),
                Box::new(extract_f64("guests_included").unwrap_or(0.0)),
                Box::new(extract_str("house_rules")),
                Box::new(extract_str("interaction")),
                Box::new(extract_opt_ts("last_review")),
                Box::new(extract_ts("last_scraped")),
                Box::new(extract_str("listing_url")),
                Box::new(extract_str("maximum_nights")),
                Box::new(extract_str("minimum_nights")),
                Box::new(extract_f64("monthly_price")),
                Box::new(extract_str("name")),
                Box::new(extract_str("neighborhood_overview")),
                Box::new(extract_str("notes")),
                Box::new(extract_int("number_of_reviews")),
                Box::new(extract_f64("price").unwrap_or(0.0)),
                Box::new(extract_str("property_type")),
                Box::new(extract_opt_int("reviews_per_month")),
                Box::new(extract_str("room_type")),
                Box::new(extract_f64("security_deposit")),
                Box::new(extract_str("space")),
                Box::new(extract_str("summary")),
                Box::new(extract_str("transit")),
                Box::new(extract_f64("weekly_price")),
            ];
            let params_ref: Vec<&(dyn ToSql + Sync)> = params
                .iter()
                .map(|value| value.as_ref() as &(dyn ToSql + Sync))
                .collect();

            pg_client.execute(&insert_sql, &params_ref[..]).await?;
        }

        Ok(Self {
            _pg_container: Box::new(pg_container),
            _mongo_container: Box::new(mongo_container),
            pg_client,
            pg_read_client,
            mongo_collection,
            schema_name,
            table_name,
            document: base_document,
        })
    }
}
