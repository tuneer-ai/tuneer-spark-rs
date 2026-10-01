pub mod adbc;
pub mod federation;
pub mod routes;

use std::net::SocketAddr;
use std::sync::Arc;

use log::info;
use tokio::net::TcpListener;

use crate::federation::FederationManager;

pub async fn serve(
    listener: TcpListener,
    federation: Arc<FederationManager>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let app = routes::build_router(federation);
    let addr = listener.local_addr()?;
    info!("Starting Sail HTTP management server on {addr}...");
    axum::serve(listener, app).await?;
    Ok(())
}

pub async fn start_http_server(
    addr: SocketAddr,
    federation: Arc<FederationManager>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind(addr).await?;
    serve(listener, federation).await
}
