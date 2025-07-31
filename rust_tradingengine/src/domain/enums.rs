use serde::{Serialize, Deserialize, Serializer};
use anyhow::{Result, anyhow};

// Note: We're still using #[serde(rename_all = "lowercase")]
// This doesn't change the type of the serialized value, just affects the value itself
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderSide {
    Buy,
    Sell,
}

// The key fix: Custom serialization to ENSURE these are always serialized as lowercase strings
// This matches what the Avro schema expects - fields defined as "String" type
impl Serialize for OrderSide {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            OrderSide::Buy => serializer.serialize_str("buy"),
            OrderSide::Sell => serializer.serialize_str("sell"),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderType {
    Limit,
    Market,
}

// Custom serialization for OrderType
impl Serialize for OrderType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            OrderType::Limit => serializer.serialize_str("limit"),
            OrderType::Market => serializer.serialize_str("market"),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TimeInForce {
    GTC,
    IOC,
}

// Custom serialization for TimeInForce
impl Serialize for TimeInForce {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            TimeInForce::GTC => serializer.serialize_str("good_till_cancelled"),
            TimeInForce::IOC => serializer.serialize_str("immediate_or_cancel"),
        }
    }
}

// Represents a domain concept (the lifecycle of an order) and should be shared across modules
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderStatus {
    Open,
    PartiallyFilled,
    Cancelled,
    CancelledPartiallyFilled,
    Filled,
}

// Custom serialization for OrderStatus to always serialize as a string
impl Serialize for OrderStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            OrderStatus::Open => serializer.serialize_str("open"),
            OrderStatus::PartiallyFilled => serializer.serialize_str("partially_filled"),
            OrderStatus::Cancelled => serializer.serialize_str("cancelled"),
            OrderStatus::CancelledPartiallyFilled => serializer.serialize_str("cancelled_partially_filled"),
            OrderStatus::Filled => serializer.serialize_str("filled"),
        }
    }
}

impl OrderStatus {
    pub fn from_str(s: &str) -> Result<Self> {
        match s {
            "open" => Ok(OrderStatus::Open),
            "partially_filled" => Ok(OrderStatus::PartiallyFilled),
            "cancelled" => Ok(OrderStatus::Cancelled),
            "cancelled_partially_filled" => Ok(OrderStatus::CancelledPartiallyFilled),
            "filled" => Ok(OrderStatus::Filled),
            _ => Err(anyhow!("Unknown order status: {}", s)),
        }
    }
}