use apache_avro::types::Value as AvroValue;
use cryptics_lab_bot::domain::enums::{OrderSide, OrderStatus, OrderType, TimeInForce};
use cryptics_lab_bot::domain::model::ack::Ack;
use cryptics_lab_bot::domain::model::trade::Trade;
use cryptics_lab_bot::domain::model::ticker::Ticker;
use cryptics_lab_bot::infrastructure::kafka::helper::AvroConverter;

#[test]
fn test_order_side_to_avro() {
    // Test Buy side
    let side = OrderSide::Buy;
    let avro_value = AvroConverter::order_side_to_avro(&side);
    
    match avro_value {
        AvroValue::Enum(i, ref s) => {
            assert_eq!(i, 0);
            assert_eq!(s, "buy");
        },
        _ => panic!("Expected Enum, got {:?}", avro_value),
    }
    
    // Test Sell side
    let side = OrderSide::Sell;
    let avro_value = AvroConverter::order_side_to_avro(&side);
    
    match avro_value {
        AvroValue::Enum(i, ref s) => {
            assert_eq!(i, 1);
            assert_eq!(s, "sell");
        },
        _ => panic!("Expected Enum, got {:?}", avro_value),
    }
}

#[test]
fn test_order_type_to_avro() {
    // Test Limit order type
    let order_type = OrderType::Limit;
    let avro_value = AvroConverter::order_type_to_avro(&order_type);
    
    match avro_value {
        AvroValue::Enum(i, ref s) => {
            assert_eq!(i, 0);
            assert_eq!(s, "limit");
        },
        _ => panic!("Expected Enum, got {:?}", avro_value),
    }
    
    // Test Market order type
    let order_type = OrderType::Market;
    let avro_value = AvroConverter::order_type_to_avro(&order_type);
    
    match avro_value {
        AvroValue::Enum(i, ref s) => {
            assert_eq!(i, 1);
            assert_eq!(s, "market");
        },
        _ => panic!("Expected Enum, got {:?}", avro_value),
    }
}

#[test]
fn test_time_in_force_to_avro() {
    // Test GTC (Good Till Cancelled)
    let tif = TimeInForce::GTC;
    let avro_value = AvroConverter::time_in_force_to_avro(&tif);
    
    match avro_value {
        AvroValue::Enum(i, ref s) => {
            assert_eq!(i, 0);
            assert_eq!(s, "good_till_cancelled");
        },
        _ => panic!("Expected Enum, got {:?}", avro_value),
    }
    
    // Test IOC (Immediate Or Cancel)
    let tif = TimeInForce::IOC;
    let avro_value = AvroConverter::time_in_force_to_avro(&tif);
    
    match avro_value {
        AvroValue::Enum(i, ref s) => {
            assert_eq!(i, 1);
            assert_eq!(s, "immediate_or_cancel");
        },
        _ => panic!("Expected Enum, got {:?}", avro_value),
    }
}

#[test]
fn test_order_status_to_avro() {
    // Test various order statuses
    let test_cases = vec![
        (OrderStatus::Open, 0, "open"),
        (OrderStatus::PartiallyFilled, 1, "partially_filled"),
        (OrderStatus::Cancelled, 2, "cancelled"),
        (OrderStatus::CancelledPartiallyFilled, 3, "cancelled_partially_filled"),
        (OrderStatus::Filled, 4, "filled"),
    ];
    
    for (status, expected_index, expected_str) in test_cases {
        let avro_value = AvroConverter::order_status_to_avro(&status);
        
        match avro_value {
            AvroValue::Enum(i, ref s) => {
                assert_eq!(i, expected_index);
                assert_eq!(s, expected_str);
            },
            _ => panic!("Expected Enum, got {:?}", avro_value),
        }
    }
}

#[test]
fn test_ack_to_avro_value() {
    // Create a test Ack object
    let ack = Ack {
        order_id: "ORD12345".to_string(),
        client_order_id: Some(67890),
        instrument_name: "BTC-PERPETUAL".to_string(),
        direction: OrderSide::Buy,
        price: Some(50000.0),
        amount: 0.1,
        filled_amount: 0.05,
        remaining_amount: 0.05,
        status: OrderStatus::PartiallyFilled,
        order_type: OrderType::Limit,
        time_in_force: TimeInForce::GTC,
        change_reason: "update".to_string(),
        delete_reason: None,
        insert_reason: Some("new_order".to_string()),
        create_time: 1645543210.123,
        persistent: true,
    };
    
    // Convert to Avro value
    let avro_fields = AvroConverter::ack_to_avro_value(&ack);
    
    // Verify all fields are present and have correct types
    assert_eq!(avro_fields.len(), 16); // There are 16 fields in the Ack struct and implementation
    
    // Verify specific fields and their values
    for (field_name, field_value) in &avro_fields {
        match field_name.as_str() {
            "order_id" => {
                match field_value {
                    AvroValue::String(s) => assert_eq!(s, "ORD12345"),
                    _ => panic!("Expected String for order_id"),
                }
            },
            "client_order_id" => {
                match field_value {
                    AvroValue::Union(1, box_value) => {
                        match &**box_value {
                            AvroValue::Long(v) => assert_eq!(*v, 67890),
                            _ => panic!("Expected Long in Union for client_order_id"),
                        }
                    },
                    _ => panic!("Expected Union for client_order_id"),
                }
            },
            "instrument_name" => {
                match field_value {
                    AvroValue::String(s) => assert_eq!(s, "BTC-PERPETUAL"),
                    _ => panic!("Expected String for instrument_name"),
                }
            },
            "direction" => {
                match field_value {
                    AvroValue::Enum(i, ref s) => {
                        assert_eq!(*i, 0);
                        assert_eq!(s, "buy");
                    },
                    _ => panic!("Expected Enum for direction"),
                }
            },
            "price" => {
                match field_value {
                    AvroValue::Union(1, box_value) => {
                        match &**box_value {
                            AvroValue::Double(v) => assert_eq!(*v, 50000.0),
                            _ => panic!("Expected Double in Union for price"),
                        }
                    },
                    _ => panic!("Expected Union for price"),
                }
            },
            "status" => {
                match field_value {
                    AvroValue::Enum(i, ref s) => {
                        assert_eq!(*i, 1);  // PartiallyFilled
                        assert_eq!(s, "partially_filled");
                    },
                    _ => panic!("Expected Enum for status"),
                }
            },
            // Add more field checks as needed
            _ => {} // Skip other fields for brevity
        }
    }
}

#[test]
fn test_trade_to_avro_value() {
    // Create a test Trade object
    let trade = Trade {
        trade_id: "TRD12345".to_string(),
        order_id: "ORD67890".to_string(),
        client_order_id: Some(42),
        instrument_name: "ETH-PERPETUAL".to_string(),
        price: 3000.0,
        amount: 0.25,
        maker_taker: "maker".to_string(),
        time: 1645543210.123,
    };
    
    // Convert to Avro value
    let avro_fields = AvroConverter::trade_to_avro_value(&trade).unwrap();
    
    // Verify all fields are present
    assert_eq!(avro_fields.len(), 8); // Ensure all fields are included
    
    // Verify specific fields and their values
    for (field_name, field_value) in &avro_fields {
        match field_name.as_str() {
            "trade_id" => {
                match field_value {
                    AvroValue::String(s) => assert_eq!(s, "TRD12345"),
                    _ => panic!("Expected String for trade_id"),
                }
            },
            "order_id" => {
                match field_value {
                    AvroValue::String(s) => assert_eq!(s, "ORD67890"),
                    _ => panic!("Expected String for order_id"),
                }
            },
            "client_order_id" => {
                match field_value {
                    AvroValue::Union(1, box_value) => {
                        match &**box_value {
                            AvroValue::Long(v) => assert_eq!(*v, 42),
                            _ => panic!("Expected Long in Union for client_order_id"),
                        }
                    },
                    _ => panic!("Expected Union for client_order_id"),
                }
            },
            "price" => {
                match field_value {
                    AvroValue::Double(v) => assert_eq!(*v, 3000.0),
                    _ => panic!("Expected Double for price"),
                }
            },
            "amount" => {
                match field_value {
                    AvroValue::Double(v) => assert_eq!(*v, 0.25),
                    _ => panic!("Expected Double for amount"),
                }
            },
            // Add more field checks as needed
            _ => {} // Skip other fields for brevity
        }
    }
    
    // Test with None for client_order_id
    let trade_without_client_id = Trade {
        trade_id: "TRD67890".to_string(),
        order_id: "ORD12345".to_string(),
        client_order_id: None,
        instrument_name: "BTC-PERPETUAL".to_string(),
        price: 50000.0,
        amount: 0.1,
        maker_taker: "taker".to_string(),
        time: 1645543210.123,
    };
    
    let avro_fields = AvroConverter::trade_to_avro_value(&trade_without_client_id).unwrap();
    
    // Check that client_order_id is a Union with Null
    for (field_name, field_value) in &avro_fields {
        if field_name == "client_order_id" {
            match field_value {
                AvroValue::Union(0, box_value) => {
                    match &**box_value {
                        AvroValue::Null => {}, // This is what we expect
                        _ => panic!("Expected Null in Union for client_order_id"),
                    }
                },
                _ => panic!("Expected Union with Null for client_order_id"),
            }
        }
    }
}

#[test]
fn test_ticker_to_avro_value() {
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
    };
    
    // Convert to Avro value
    let avro_fields = AvroConverter::ticker_to_avro_value(&ticker).unwrap();
    
    // Verify all fields are present
    assert_eq!(avro_fields.len(), 23); // Ensure all fields are included
    
    // Verify specific fields and their values
    for (field_name, field_value) in &avro_fields {
        match field_name.as_str() {
            "instrument_name" => {
                match field_value {
                    AvroValue::String(s) => assert_eq!(s, "BTC-PERPETUAL"),
                    _ => panic!("Expected String for instrument_name"),
                }
            },
            "mark_price" => {
                match field_value {
                    AvroValue::Double(v) => assert_eq!(*v, 50000.0),
                    _ => panic!("Expected Double for mark_price"),
                }
            },
            "best_bid_price" => {
                match field_value {
                    AvroValue::Double(v) => assert_eq!(*v, 49950.0),
                    _ => panic!("Expected Double for best_bid_price"),
                }
            },
            "best_ask_price" => {
                match field_value {
                    AvroValue::Double(v) => assert_eq!(*v, 50050.0),
                    _ => panic!("Expected Double for best_ask_price"),
                }
            },
            "last_price" => {
                match field_value {
                    AvroValue::Double(v) => assert_eq!(*v, 49975.0),
                    _ => panic!("Expected Double for last_price"),
                }
            },
            "index_price" => {
                match field_value {
                    AvroValue::Double(v) => assert_eq!(*v, 50010.0),
                    _ => panic!("Expected Double for index_price"),
                }
            },
            "funding_rate" => {
                match field_value {
                    AvroValue::Double(v) => assert_eq!(*v, 0.0002),
                    _ => panic!("Expected Double for funding_rate"),
                }
            },
            // Add more field checks as needed
            _ => {} // Skip other fields for brevity
        }
    }
}