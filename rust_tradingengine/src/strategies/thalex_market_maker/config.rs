/// Constants and configuration parameters for the Thalex market maker
pub const PING_INTERVAL_SEC: u64 = 5;
pub const TIMEOUT_SEC: u64 = 6;
pub const TYPE: &str = "perpetual";
pub const UNDERLYING: &str = "BTCUSD";
pub const LABEL: &str = "P";
pub const AMEND_THRESHOLD: f64 = 5.0;
pub const SPREAD: f64 = 25.0;
pub const BID_STEP: f64 = 5.0;
pub const BID_SIZES: &[f64] = &[0.2, 0.4];
pub const ASK_STEP: f64 = 5.0;
pub const ASK_SIZES: &[f64] = &[0.2, 0.4];

/// WebSocket channels to subscribe
pub const CHANNELS: &[&str] = &[
    "session.orders",
    "account.portfolio",
    "account.trade_history",
];
