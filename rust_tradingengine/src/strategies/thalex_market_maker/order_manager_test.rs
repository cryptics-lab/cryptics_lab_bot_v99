#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::constants;
    use crate::infrastructure::exchange::thalex::models::{
        Ticker, Index, OrderAcknowledgement, Order
    };
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use tokio::time::{Duration, Instant};
    use mockall::predicate::*;
    use mockall::mock;

    // Mock the ThalexClient for testing
    mock! {
        ThalexClient {}
        impl ThalexClient {
            pub async fn send_order(&mut self, order: Order, call_id: Option<u64>) -> Result<(), anyhow::Error>;
            pub async fn amend_order(&mut self, order_id: &str, price: f64, amount: f64, call_id: Option<u64>) -> Result<(), anyhow::Error>;
            pub async fn cancel_order(&mut self, order_id: &str, call_id: Option<u64>) -> Result<(), anyhow::Error>;
        }
    }

    // Create a mock MarketDataManager for testing
    fn create_mock_market_data() -> Arc<MarketDataManager> {
        let notify = Arc::new(tokio::sync::Notify::new());
        let market_data = MarketDataManager::new(notify, None);
        
        // Manually set values for testing
        let ticker = Ticker {
            instrument_name: "BTCUSD-PERPETUAL".to_string(),
            mark_price: 50000.0,
            best_bid_price: 49950.0,
            best_ask_price: 50050.0,
            // Fill in other required fields...
            mark_timestamp: 0.0,
            best_bid_amount: 0.0,
            best_ask_amount: 0.0,
            last_price: 0.0,
            delta: 0.0,
            volume_24h: 0.0,
            value_24h: 0.0,
            low_price_24h: 0.0,
            high_price_24h: 0.0,
            change_24h: 0.0,
            index_price: 0.0,
            forward: 0.0,
            funding_mark: 0.0,
            funding_rate: 0.0,
            collar_low: 0.0,
            collar_high: 0.0,
            realised_funding_24h: 0.0,
            average_funding_rate_24h: 0.0,
            open_interest: 0.0,
        };
        
        let index = Index {
            index_name: "BTCUSD".to_string(),
            price: 50000.0,
            // Fill in other required fields...
            timestamp: 0.0,
            previous_settlement_price: 0.0,
        };
        
        // Use unwrap_or_default for tests
        market_data.set_ticker(ticker).await.unwrap_or_default();
        market_data.set_index_price(index).await.unwrap_or_default();
        market_data.set_instrument_info("BTCUSD-PERPETUAL".to_string(), 0.1).await.unwrap_or_default();
        
        Arc::new(market_data)
    }

    #[tokio::test]
    async fn test_make_quotes_generates_correct_quotes() {
        // Arrange
        let mock_client = Arc::new(Mutex::new(MockThalexClient::new()));
        let market_data = create_mock_market_data();
        let order_manager = Arc::new(OrderManager::new(
            mock_client.clone(),
            market_data.clone(),
            None
        ));
        
        // Act
        let quotes = order_manager.make_quotes().await.unwrap();
        
        // Assert
        // We should have quotes on both sides of the book
        assert!(!quotes.bids.is_empty(), "Should have at least one bid");
        assert!(!quotes.asks.is_empty(), "Should have at least one ask");
        
        // Bids should be below current price
        for bid in &quotes.bids {
            assert!(bid.price < 50000.0, "Bid price should be below mark price");
            assert!(bid.amount > 0.0, "Bid amount should be positive");
        }
        
        // Asks should be above current price
        for ask in &quotes.asks {
            assert!(ask.price > 50000.0, "Ask price should be above mark price");
            assert!(ask.amount > 0.0, "Ask amount should be positive");
        }
        
        // Check that the spread is at least the minimum configured value
        if !quotes.bids.is_empty() && !quotes.asks.is_empty() {
            let min_ask = quotes.asks.iter().map(|a| a.price).fold(f64::MAX, f64::min);
            let max_bid = quotes.bids.iter().map(|b| b.price).fold(0.0, f64::max);
            let spread = min_ask - max_bid;
            
            assert!(spread >= config::SPREAD, 
                    "Spread between best bid and ask should be at least the configured minimum");
        }
    }

    #[tokio::test]
    async fn test_adjust_quotes_sends_correct_orders() {
        // Arrange
        let mut mock_client = MockThalexClient::new();
        
        // Expect send_order to be called for new quotes
        mock_client.expect_send_order()
            .times(4)  // 2 bids + 2 asks
            .returning(|_, _| Ok(()));
            
        let mock_client = Arc::new(Mutex::new(mock_client));
        let market_data = create_mock_market_data();
        let order_manager = Arc::new(OrderManager::new(
            mock_client.clone(),
            market_data.clone(),
            None
        ));
        
        // Generate test quotes
        let quotes = Quotes {
            bids: vec![
                Quote { price: 49950.0, amount: 0.2 },
                Quote { price: 49945.0, amount: 0.4 },
            ],
            asks: vec![
                Quote { price: 50050.0, amount: 0.2 },
                Quote { price: 50055.0, amount: 0.4 },
            ],
        };
        
        // Act
        let result = order_manager.adjust_quotes(quotes).await;
        
        // Assert
        assert!(result.is_ok(), "adjust_quotes should succeed");
    }

    #[tokio::test]
    async fn test_adjust_quotes_amends_existing_orders() {
        // Arrange
        let mut mock_client = MockThalexClient::new();
        
        // Expect amend_order to be called for existing orders
        mock_client.expect_amend_order()
            .times(1)
            .returning(|_, _, _, _| Ok(()));
            
        // Expect send_order to be called for new quotes
        mock_client.expect_send_order()
            .times(3)
            .returning(|_, _| Ok(()));
            
        let mock_client = Arc::new(Mutex::new(mock_client));
        let market_data = create_mock_market_data();
        let order_manager = Arc::new(OrderManager::new(
            mock_client.clone(),
            market_data.clone(),
            None
        ));
        
        // Add an existing order to the order manager
        let existing_order = OrderAcknowledgement {
            order_id: "existing-order-123".to_string(),
            instrument_name: "BTCUSD-PERPETUAL".to_string(),
            price: 49950.0,
            amount: 0.2,
            direction: "buy".to_string(),
            // Fill in other required fields...
            client_order_id: 0,
            filled_amount: 0.0,
            remaining_amount: 0.2,
            status: "open".to_string(),
            order_type: "".to_string(),
            time_in_force: "".to_string(),
            change_reason: "".to_string(),
            delete_reason: None,
            insert_reason: None,
            create_time: 0.0,
            persistent: false,
        };
        
        order_manager.update_order_status(existing_order).await.unwrap();
        
        // Generate test quotes with one matching the existing order but with a different price
        let quotes = Quotes {
            bids: vec![
                Quote { price: 49955.0, amount: 0.2 },  // Same amount but different price -> should amend
                Quote { price: 49945.0, amount: 0.4 },  // New order
            ],
            asks: vec![
                Quote { price: 50050.0, amount: 0.2 },  // New order
                Quote { price: 50055.0, amount: 0.4 },  // New order
            ],
        };
        
        // Act
        let result = order_manager.adjust_quotes(quotes).await;
        
        // Assert
        assert!(result.is_ok(), "adjust_quotes should succeed with existing orders");
    }
}
