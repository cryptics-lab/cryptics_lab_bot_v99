use crate::core::model::*;
use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub trait TradingClient {
    async fn place_order(&mut self, order: OrderRequest) -> Result<()>;
    async fn cancel_order(&mut self, id: u64) -> Result<()>;
}

#[async_trait]
pub trait MarketDataClient {
    async fn get_instruments(&mut self) -> Result<Vec<Instrument>>;
    async fn subscribe_public(&mut self, channels: Vec<String>) -> Result<()>;
    async fn subscribe_private(&mut self, channels: Vec<String>) -> Result<()>;
}

#[async_trait]
pub trait AdminClient {
    async fn connect(&mut self) -> Result<()>;
    async fn disconnect(&mut self) -> Result<()>;
    fn is_connected(&self) -> bool;
    async fn login(&mut self, token: String, account: Option<String>) -> Result<()>;
    async fn receive(&mut self) -> Result<Option<String>>;
    async fn set_cancel_on_disconnect(&mut self, timeout_secs: u64) -> Result<()>;
}
