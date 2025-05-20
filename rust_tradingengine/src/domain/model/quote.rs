// Domain model for quotes (bid/ask price + amount pairs)

/// Represents a single price level with associated amount
#[derive(Clone, Debug)]
pub struct SideQuote {
    pub price: f64,
    pub amount: f64,
}

impl SideQuote {
    pub fn new(price: f64, amount: f64) -> Self {
        Self { price, amount }
    }
}
