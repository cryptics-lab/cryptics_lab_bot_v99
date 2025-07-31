//! Thalex Market Maker Strategy Module
//!
//! This module contains the full strategy logic for market making on Thalex,
//! including market data handling, order management, quoting, and message routing.

mod config;
mod market_data;
mod order_manager;
mod notification_handler;
pub mod quoter; // contains ThalexQuoter runner

// Re-export core strategy components
pub use config::*;
pub use market_data::MarketDataManager;
pub use order_manager::OrderManager;
pub use notification_handler::NotificationHandler;
pub use quoter::ThalexQuoter;
