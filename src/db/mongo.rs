//! MongoDB connector: client construction shared across command handlers.

use anyhow::Result;
use mongodb::options::ClientOptions;
use mongodb::Client;

/// Parse a MongoDB connection string into `ClientOptions`.
///
/// Kept separate from [`client_with_options`] so callers can attach distinct
/// error context to the "parse" and "connect" failure modes, matching the
/// behavior each command handler had before this connector was extracted.
pub async fn parse_client_options(uri: &str) -> Result<ClientOptions> {
    Ok(ClientOptions::parse(uri).await?)
}

/// Construct a `Client` from already-parsed `ClientOptions`.
pub fn client_with_options(options: ClientOptions) -> Result<Client> {
    Ok(Client::with_options(options)?)
}

/// Convenience helper combining [`parse_client_options`] and [`client_with_options`]
/// for callers that don't need distinct error context for each step.
pub async fn connect_client(uri: &str) -> Result<Client> {
    let options = parse_client_options(uri).await?;
    client_with_options(options)
}
