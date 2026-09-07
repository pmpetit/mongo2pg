//! Kafka connector: `ClientConfig` security/TLS settings shared by the `ping`
//! command handler and the (binary-local) `kafka-import` worker.

use rdkafka::ClientConfig;

use crate::util::KafkaConfData;

/// Well-known system CA bundle locations checked when the configured security
/// protocol implies SSL/TLS but no explicit `ssl_ca_location` was set.
const DEFAULT_SSL_CA_LOCATION_CANDIDATES: [&str; 3] = [
    "/etc/ssl/certs/ca-certificates.crt",
    "/etc/pki/tls/certs/ca-bundle.crt",
    "/etc/ssl/cert.pem",
];

/// Detect a default CA bundle path when the security protocol implies SSL and no
/// explicit `ssl_ca_location` was configured.
pub fn detect_default_ssl_ca_location(kafka_conf: &KafkaConfData) -> Option<String> {
    let security_protocol = kafka_conf
        .security_protocol
        .as_deref()
        .map(str::trim)
        .unwrap_or("")
        .to_ascii_uppercase();
    let ssl_ca_location_configured = kafka_conf
        .ssl_ca_location
        .as_deref()
        .map(str::trim)
        .is_some_and(|value| !value.is_empty());

    if security_protocol.contains("SSL") && !ssl_ca_location_configured {
        DEFAULT_SSL_CA_LOCATION_CANDIDATES
            .iter()
            .find(|candidate| std::path::Path::new(candidate).is_file())
            .map(|candidate| (*candidate).to_owned())
    } else {
        None
    }
}

/// Apply security-related settings (security.protocol, SASL, SSL) from kafka config
/// onto a `ClientConfig`. `default_ssl_ca_location` should come from
/// [`detect_default_ssl_ca_location`] and is used as a fallback when
/// `kafka_conf.ssl_ca_location` is not set.
pub fn apply_security_config(
    client_config: &mut ClientConfig,
    kafka_conf: &KafkaConfData,
    default_ssl_ca_location: Option<&str>,
) {
    if let Some(value) = kafka_conf
        .security_protocol
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        client_config.set("security.protocol", value);
    }
    if let Some(value) = kafka_conf
        .sasl_mechanism
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        client_config.set("sasl.mechanism", value);
    }
    if let Some(value) = kafka_conf
        .sasl_username
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        client_config.set("sasl.username", value);
    }
    if let Some(value) = kafka_conf
        .sasl_password
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        client_config.set("sasl.password", value);
    }
    if let Some(value) = kafka_conf
        .ssl_ca_location
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        client_config.set("ssl.ca.location", value);
    } else if let Some(value) = default_ssl_ca_location {
        client_config.set("ssl.ca.location", value);
    }
    if let Some(value) = kafka_conf
        .ssl_certificate_location
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        client_config.set("ssl.certificate.location", value);
    }
    if let Some(value) = kafka_conf
        .ssl_key_location
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        client_config.set("ssl.key.location", value);
    }
    if let Some(value) = kafka_conf
        .ssl_key_password
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        client_config.set("ssl.key.password", value);
    }
}
