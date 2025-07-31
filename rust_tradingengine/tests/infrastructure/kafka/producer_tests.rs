use std::collections::HashMap;
use anyhow::Result;
use serde_json::json;

use cryptics_lab_bot::domain::model::ticker::Ticker;
use cryptics_lab_bot::infrastructure::kafka::helper::AvroConverter;
use cryptics_lab_bot::infrastructure::exchange::thalex::parsers::ThaleParser;

// Test for the AvroConverter and the Ticker model
#[test]
fn test_ticker_to_avro_fields() -> Result<()> {
    // Create a test Ticker
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
    
    // Test that we can convert the ticker to Avro fields without error
    let avro_fields = AvroConverter::ticker_to_avro_value(&ticker)?;
    
    // Verify the fields exist and have the right values
    assert_eq!(avro_fields.len(), 24); // Ensure all fields are included (including processing_timestamp)
    
    // Check some of the key fields
    let field_map: HashMap<_, _> = avro_fields.iter().cloned().collect();
    
    match &field_map["instrument_name"] {
        apache_avro::types::Value::String(s) => assert_eq!(s, "BTC-PERPETUAL"),
        _ => panic!("Expected String for instrument_name"),
    }
    
    match &field_map["mark_price"] {
        apache_avro::types::Value::Double(v) => assert_eq!(*v, 50000.0),
        _ => panic!("Expected Double for mark_price"),
    }
    
    match &field_map["index_price"] {
        apache_avro::types::Value::Double(v) => assert_eq!(*v, 50010.0),
        _ => panic!("Expected Double for index_price"),
    }
    
    match &field_map["funding_rate"] {
        apache_avro::types::Value::Double(v) => assert_eq!(*v, 0.0002),
        _ => panic!("Expected Double for funding_rate"),
    }
    
    Ok(())
}

// Test for the ThaleParser and Trade model
#[test]
fn test_trade_from_json() -> Result<()> {
    // Create sample JSON data for a direct trade
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
    
    // Now test converting the Trade to Avro
    let avro_fields = AvroConverter::trade_to_avro_value(&trade)?;
    
    // Verify all fields are present
    assert_eq!(avro_fields.len(), 9); // Ensure all fields are included (including processing_timestamp)
    
    // Check some key fields
    let field_map: HashMap<_, _> = avro_fields.iter().cloned().collect();
    
    match &field_map["trade_id"] {
        apache_avro::types::Value::String(s) => assert_eq!(s, "trd-123456"),
        _ => panic!("Expected String for trade_id"),
    }
    
    match &field_map["price"] {
        apache_avro::types::Value::Double(v) => assert_eq!(*v, 50000.0),
        _ => panic!("Expected Double for price"),
    }
    
    match &field_map["client_order_id"] {
        apache_avro::types::Value::Union(1, box_value) => {
            match &**box_value {
                apache_avro::types::Value::Long(v) => assert_eq!(*v, 42),
                _ => panic!("Expected Long in Union for client_order_id"),
            }
        },
        _ => panic!("Expected Union for client_order_id"),
    }
    
    // Convert back to JSON to test the roundtrip
    let json = ThaleParser::trade_to_json(&trade);
    
    assert_eq!(json["trade_id"], "trd-123456");
    assert_eq!(json["price"], 50000.0);
    assert_eq!(json["client_order_id"], 42);
    
    Ok(())
}

// Test extracting trades from an order with fills
#[test]
fn test_extract_trades_from_order() -> Result<()> {
    // Create sample order data with fills
    let order_data = json!({
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
    
    // Parse the Ack and extract trades
    let ack = ThaleParser::parse_ack_json(&order_data)?;
    let trades = ThaleParser::extract_trades_from_order(&order_data)?;
    
    // Verify the Ack
    assert_eq!(ack.order_id, "ord-123456");
    assert_eq!(ack.client_order_id, Some(42));
    assert_eq!(ack.instrument_name, "BTC-PERPETUAL");
    assert_eq!(ack.amount, 0.2);
    
    // Verify the extracted trades
    assert_eq!(trades.len(), 2);
    assert_eq!(trades[0].trade_id, "trd-111111");
    assert_eq!(trades[0].price, 50000.0);
    assert_eq!(trades[0].amount, 0.1);
    
    assert_eq!(trades[1].trade_id, "trd-222222");
    assert_eq!(trades[1].amount, 0.1);
    
    Ok(())
}
