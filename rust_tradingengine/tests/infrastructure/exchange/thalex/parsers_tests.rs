use anyhow::Result;
use serde_json::json;
use cryptics_lab_bot::domain::enums::{OrderSide, OrderStatus, OrderType, TimeInForce};
use cryptics_lab_bot::domain::model::trade::Trade;
use cryptics_lab_bot::infrastructure::exchange::thalex::parsers::ThaleParser;

#[test]
fn test_parse_ack_json() -> Result<()> {
    // Create sample JSON data
    let json_data = json!({
        "order_id": "ord-123456",
        "client_order_id": 42,
        "instrument_name": "BTC-PERPETUAL",
        "direction": "buy",
        "price": 50000.0,
        "amount": 0.1,
        "filled_amount": 0.05,
        "remaining_amount": 0.05,
        "status": "partially_filled",
        "order_type": "limit",
        "time_in_force": "good_till_cancelled",
        "change_reason": "update",
        "delete_reason": null,
        "insert_reason": "new_order",
        "create_time": 1645543210.123,
        "persistent": true
    });
    
    // Parse the JSON into an Ack
    let ack = ThaleParser::parse_ack_json(&json_data)?;
    
    // Verify the fields
    assert_eq!(ack.order_id, "ord-123456");
    assert_eq!(ack.client_order_id, Some(42));
    assert_eq!(ack.instrument_name, "BTC-PERPETUAL");
    
    match ack.direction {
        OrderSide::Buy => {}, // Expected
        _ => panic!("Expected Buy direction"),
    }
    
    assert_eq!(ack.price, Some(50000.0));
    assert_eq!(ack.amount, 0.1);
    assert_eq!(ack.filled_amount, 0.05);
    assert_eq!(ack.remaining_amount, 0.05);
    
    match ack.status {
        OrderStatus::PartiallyFilled => {}, // Expected
        _ => panic!("Expected PartiallyFilled status"),
    }
    
    match ack.order_type {
        OrderType::Limit => {}, // Expected
        _ => panic!("Expected Limit order type"),
    }
    
    match ack.time_in_force {
        TimeInForce::GTC => {}, // Expected
        _ => panic!("Expected GTC time in force"),
    }
    
    assert_eq!(ack.change_reason, "update");
    assert_eq!(ack.delete_reason, None);
    assert_eq!(ack.insert_reason, Some("new_order".to_string()));
    assert_eq!(ack.create_time, 1645543210.123);
    assert_eq!(ack.persistent, true);
    
    // Test with missing fields or different values
    let json_data_with_nulls = json!({
        "order_id": "ord-789012",
        "client_order_id": null,
        "instrument_name": "ETH-PERPETUAL",
        "direction": "sell",
        "price": null,
        "amount": 0.5,
        "filled_amount": 0.0,
        "remaining_amount": 0.5,
        "status": "open",
        "order_type": "market",
        "time_in_force": "immediate_or_cancel",
        "change_reason": "new",
        "delete_reason": "user_requested",
        "insert_reason": null,
        "create_time": 1645543220.456,
        "persistent": false
    });
    
    let ack_with_nulls = ThaleParser::parse_ack_json(&json_data_with_nulls)?;
    
    assert_eq!(ack_with_nulls.order_id, "ord-789012");
    assert_eq!(ack_with_nulls.client_order_id, None);
    assert_eq!(ack_with_nulls.instrument_name, "ETH-PERPETUAL");
    
    match ack_with_nulls.direction {
        OrderSide::Sell => {}, // Expected
        _ => panic!("Expected Sell direction"),
    }
    
    assert_eq!(ack_with_nulls.price, None);
    
    match ack_with_nulls.status {
        OrderStatus::Open => {}, // Expected
        _ => panic!("Expected Open status"),
    }
    
    match ack_with_nulls.order_type {
        OrderType::Market => {}, // Expected
        _ => panic!("Expected Market order type"),
    }
    
    match ack_with_nulls.time_in_force {
        TimeInForce::IOC => {}, // Expected
        _ => panic!("Expected IOC time in force"),
    }
    
    assert_eq!(ack_with_nulls.delete_reason, Some("user_requested".to_string()));
    assert_eq!(ack_with_nulls.insert_reason, None);
    
    Ok(())
}

#[test]
fn test_parse_ticker_json() -> Result<()> {
    use cryptics_lab_bot::domain::model::ticker::Ticker;
    
    // Create a test Ticker object
    let ticker = Ticker {
        instrument_name: "BTC-PERPETUAL".to_string(),
        mark_price: 50000.0,
        mark_timestamp: 1645543210.123,
        best_bid_price: 49950.0,
        best_bid_amount: 0.5,
        best_ask_price: 50050.0,
        best_ask_amount: 0.3,
        last_price: 49975.0,
        delta: 0.1,
        volume_24h: 1000.0,
        value_24h: 50000000.0,
        low_price_24h: 48000.0,
        high_price_24h: 51000.0,
        change_24h: 2.5,
        index_price: 50010.0,
        forward: 0.0,
        funding_mark: 0.0001,
        funding_rate: 0.0002,
        collar_low: 48000.0,
        collar_high: 52000.0,
        realised_funding_24h: 0.0003,
        average_funding_rate_24h: 0.0002,
        open_interest: 500.0,
        processing_timestamp: Some(1645543210.456),
    };
    
    // Parse the ticker to JSON
    let ticker_json = ThaleParser::parse_ticker_json(&ticker)?;
    
    // Verify key fields in the JSON
    assert_eq!(ticker_json["instrument_name"], "BTC-PERPETUAL");
    assert_eq!(ticker_json["timestamp"], 1645543210.123);
    assert_eq!(ticker_json["last_price"], 49975.0);
    assert_eq!(ticker_json["mark_price"], 50000.0);
    assert_eq!(ticker_json["index_price"], 50010.0);
    assert_eq!(ticker_json["funding_rate"], 0.0002);
    assert_eq!(ticker_json["open_interest"], 500.0);
    
    // Ensure the JSON has the expected number of fields
    // The parser only extracts a subset of fields for the API payload (including processing_timestamp)
    assert_eq!(ticker_json.as_object().unwrap().len(), 8);
    
    Ok(())
}

#[test]
fn test_parse_trade_json() -> Result<()> {
    // Create sample JSON data
    let json_data = json!({
        "trade_id": "trd-123456",
        "order_id": "ord-789012",
        "client_order_id": 42,
        "instrument_name": "BTC-PERPETUAL",
        "price": 50000.0,
        "amount": 0.1,
        "maker_taker": "maker",
        "timestamp": 1645543210.123
    });
    
    // Parse the JSON into a Trade
    let trade = ThaleParser::parse_trade_json(&json_data)?;
    
    // Verify the fields
    assert_eq!(trade.trade_id, "trd-123456");
    assert_eq!(trade.order_id, "ord-789012");
    assert_eq!(trade.client_order_id, Some(42));
    assert_eq!(trade.instrument_name, "BTC-PERPETUAL");
    assert_eq!(trade.price, 50000.0);
    assert_eq!(trade.amount, 0.1);
    assert_eq!(trade.maker_taker, "maker");
    assert_eq!(trade.time, 1645543210.123);
    
    // Test with missing trade_id (should generate a UUID)
    let json_data_without_trade_id = json!({
        "order_id": "ord-345678",
        "client_order_id": null,
        "instrument_name": "ETH-PERPETUAL",
        "price": 3000.0,
        "amount": 0.5,
        "maker_taker": "taker",
        "timestamp": 1645543220.456
    });
    
    let trade_without_id = ThaleParser::parse_trade_json(&json_data_without_trade_id)?;
    
    // Verify that a trade_id was generated (it should start with "trade-")
    assert!(trade_without_id.trade_id.starts_with("trade-"));
    assert_eq!(trade_without_id.order_id, "ord-345678");
    assert_eq!(trade_without_id.client_order_id, None);
    assert_eq!(trade_without_id.instrument_name, "ETH-PERPETUAL");
    assert_eq!(trade_without_id.price, 3000.0);
    assert_eq!(trade_without_id.amount, 0.5);
    assert_eq!(trade_without_id.maker_taker, "taker");
    assert_eq!(trade_without_id.time, 1645543220.456);
    
    Ok(())
}

#[test]
fn test_extract_trades_from_order() -> Result<()> {
    // Create sample order data with fills
    let order_data_with_fills = json!({
        "order_id": "ord-123456",
        "client_order_id": 42,
        "instrument_name": "BTC-PERPETUAL",
        "direction": "buy",
        "price": 50000.0,
        "amount": 0.2,
        "filled_amount": 0.2,
        "remaining_amount": 0.0,
        "status": "filled",
        "order_type": "limit",
        "time_in_force": "good_till_cancelled",
        "change_reason": "fill",
        "create_time": 1645543210.123,
        "persistent": true,
        "fills": [
            {
                "trade_id": "trd-111111",
                "price": 50000.0,
                "amount": 0.1,
                "maker_taker": "maker",
                "time": 1645543220.456
            },
            {
                "trade_id": "trd-222222",
                "price": 50000.0,
                "amount": 0.1,
                "maker_taker": "maker",
                "time": 1645543230.789
            }
        ]
    });
    
    // Extract trades from the order data
    let trades = ThaleParser::extract_trades_from_order(&order_data_with_fills)?;
    
    // Verify the extracted trades
    assert_eq!(trades.len(), 2);
    
    // Check first trade
    assert_eq!(trades[0].trade_id, "trd-111111");
    assert_eq!(trades[0].order_id, "ord-123456");
    assert_eq!(trades[0].client_order_id, Some(42));
    assert_eq!(trades[0].instrument_name, "BTC-PERPETUAL");
    assert_eq!(trades[0].price, 50000.0);
    assert_eq!(trades[0].amount, 0.1);
    assert_eq!(trades[0].maker_taker, "maker");
    assert_eq!(trades[0].time, 1645543220.456);
    
    // Check second trade
    assert_eq!(trades[1].trade_id, "trd-222222");
    assert_eq!(trades[1].order_id, "ord-123456");
    assert_eq!(trades[1].client_order_id, Some(42));
    assert_eq!(trades[1].instrument_name, "BTC-PERPETUAL");
    assert_eq!(trades[1].price, 50000.0);
    assert_eq!(trades[1].amount, 0.1);
    assert_eq!(trades[1].maker_taker, "maker");
    assert_eq!(trades[1].time, 1645543230.789);
    
    // Test with missing trade_id (should generate a UUID)
    let order_data_without_trade_ids = json!({
        "order_id": "ord-345678",
        "client_order_id": null,
        "instrument_name": "ETH-PERPETUAL",
        "direction": "sell",
        "price": 3000.0,
        "amount": 0.5,
        "filled_amount": 0.5,
        "remaining_amount": 0.0,
        "status": "filled",
        "order_type": "limit",
        "time_in_force": "good_till_cancelled",
        "change_reason": "fill",
        "create_time": 1645543240.123,
        "persistent": true,
        "fills": [
            {
                "price": 3000.0,
                "amount": 0.3,
                "maker_taker": "taker",
                "time": 1645543250.456
            },
            {
                "price": 3000.0,
                "amount": 0.2,
                "maker_taker": "taker",
                "time": 1645543260.789
            }
        ]
    });
    
    let trades_without_ids = ThaleParser::extract_trades_from_order(&order_data_without_trade_ids)?;
    
    // Verify trades were extracted
    assert_eq!(trades_without_ids.len(), 2);
    
    // Check generated trade IDs
    assert!(trades_without_ids[0].trade_id.starts_with("trade-"));
    assert!(trades_without_ids[1].trade_id.starts_with("trade-"));
    
    // Test with an order that has no fills
    let order_data_without_fills = json!({
        "order_id": "ord-999999",
        "client_order_id": 99,
        "instrument_name": "BTC-PERPETUAL",
        "direction": "buy",
        "price": 50000.0,
        "amount": 0.1,
        "filled_amount": 0.0,
        "remaining_amount": 0.1,
        "status": "open",
        "order_type": "limit",
        "time_in_force": "good_till_cancelled",
        "change_reason": "new",
        "create_time": 1645543270.123,
        "persistent": true
    });
    
    let trades_without_fills = ThaleParser::extract_trades_from_order(&order_data_without_fills)?;
    
    // There should be no trades
    assert_eq!(trades_without_fills.len(), 0);
    
    Ok(())
}

#[test]
fn test_trade_to_json() {
    // Create a test Trade object
    let trade = Trade {
        trade_id: "trd-123456".to_string(),
        order_id: "ord-789012".to_string(),
        client_order_id: Some(42),
        instrument_name: "BTC-PERPETUAL".to_string(),
        price: 50000.0,
        amount: 0.1,
        maker_taker: "maker".to_string(),
        time: 1645543210.123,
        processing_timestamp: Some(1645543210.456),
    };
    
    // Convert to JSON
    let json = ThaleParser::trade_to_json(&trade);
    
    // Verify JSON fields
    assert_eq!(json["trade_id"], "trd-123456");
    assert_eq!(json["order_id"], "ord-789012");
    assert_eq!(json["client_order_id"], 42);
    assert_eq!(json["instrument_name"], "BTC-PERPETUAL");
    assert_eq!(json["price"], 50000.0);
    assert_eq!(json["amount"], 0.1);
    assert_eq!(json["maker_taker"], "maker");
    assert_eq!(json["time"], 1645543210.123);
    
    // Test with None for client_order_id
    let trade_without_client_id = Trade {
        trade_id: "trd-234567".to_string(),
        order_id: "ord-890123".to_string(),
        client_order_id: None,
        instrument_name: "ETH-PERPETUAL".to_string(),
        price: 3000.0,
        amount: 0.5,
        maker_taker: "taker".to_string(),
        time: 1645543220.456,
        processing_timestamp: Some(1645543220.789),
    };
    
    let json_without_client_id = ThaleParser::trade_to_json(&trade_without_client_id);
    
    // Verify client_order_id is null
    assert!(json_without_client_id["client_order_id"].is_null());
}
