"""
Tests for producer business logic.
"""

import asyncio
import datetime
import random
import unittest
from typing import Any, Dict, List

from python_pipeline.data_generators.ack_generator import AckDataGenerator
from python_pipeline.data_generators.base_generator import BaseDataGenerator
from python_pipeline.models.ack import (
    OrderSide,
    OrderStatus,
    OrderType,
    ThalexAck,
    TimeInForce,
)
from python_pipeline.models.ticker import ThalexTicker
from python_pipeline.models.trade import MakerTakerRole, ThalexTrade
from python_pipeline.producers.ack_producer import AckProducer
from python_pipeline.producers.ticker_producer import TickerProducer
from python_pipeline.producers.trade_producer import TradeProducer
from tests.base_test import AvroSerializationTestBase


# Create test data generators for models that don't have dedicated generators yet
class TickerDataGenerator(BaseDataGenerator):
    """Test generator for Ticker data"""
    
    def generate(self, **kwargs) -> ThalexTicker:
        """Generate sample ticker data"""
        base_price = random.uniform(95000, 97000)
        bid_price = base_price - random.uniform(10, 30)
        ask_price = base_price + random.uniform(10, 30)
        
        return ThalexTicker(
            mark_price=base_price,
            mark_timestamp=datetime.datetime.now().timestamp(),
            best_bid_price=bid_price,
            best_bid_amount=random.uniform(0.01, 0.1),
            best_ask_price=ask_price,
            best_ask_amount=random.uniform(0.01, 0.1),
            last_price=base_price + random.uniform(-50, 50),
            delta=1.0,
            volume_24h=random.uniform(700, 800),
            value_24h=random.uniform(68000000, 69000000),
            low_price_24h=random.uniform(93000, 94000),
            high_price_24h=random.uniform(97000, 98000),
            change_24h=random.uniform(2000, 3500),
            index_price=base_price - random.uniform(-10, 10),
            forward=base_price - random.uniform(-10, 10),
            funding_mark=random.uniform(0, 0.001),
            funding_rate=random.uniform(0, 0.001),
            collar_low=base_price * 0.99,
            collar_high=base_price * 1.01,
            realised_funding_24h=random.uniform(0, 0.001),
            average_funding_rate_24h=random.uniform(0, 0.001),
            open_interest=random.uniform(3700, 3800)
        )

class TradeDataGenerator(BaseDataGenerator):
    """Test generator for Trade data"""
    
    def generate(self, **kwargs) -> ThalexTrade:
        """Generate sample trade data"""
        # Use provided order_id if available, otherwise generate one
        if 'order_id' in kwargs:
            order_id = kwargs['order_id']
        else:
            order_id = f"order_{random.randint(10000, 99999)}"
            
        # Generate random but plausible values
        return ThalexTrade(
            trade_id=f"trade_{random.randint(10000, 99999)}",
            order_id=order_id,
            client_order_id=kwargs.get('client_order_id', random.randint(100, 999)),
            instrument_name=kwargs.get('instrument_name', "BTC-PERPETUAL"),
            price=random.uniform(95000, 97000),
            amount=random.uniform(0.1, 1.0),
            maker_taker=random.choice([MakerTakerRole.MAKER, MakerTakerRole.TAKER]),
            time=kwargs.get('timestamp', datetime.datetime.now().timestamp())
        )


class MockConfig:
    """Mock configuration for testing producers"""
    
    @staticmethod
    def get_config() -> Dict[str, Any]:
        """Get a minimal configuration for testing"""
        return {
            'pipeline': {
                'python_running_in_docker': False,
                'use_schema_files': True,
                'enabled_models': ['ticker', 'ack', 'trade']
            },
            'kafka': {
                'bootstrap_servers': 'localhost:9092',
                'schema_registry_url': 'http://localhost:8081',
                'topic_partitions': 1,
                'topic_replicas': 1
            },
            'topics': {
                'ticker': 'test.cryptics.thalex.ticker.avro',
                'ack': 'test.cryptics.thalex.ack.avro',
                'trade': 'test.cryptics.thalex.trade.avro',
                'base_name': 'test.cryptics.thalex'
            }
        }


class MockAckProducer(AckProducer):
    """Mock AckProducer that doesn't connect to Kafka"""
    
    def __init__(self):
        """Initialize with mock config and don't connect to Kafka"""
        super().__init__(MockConfig.get_config())
        self.produced_acks: List[ThalexAck] = []
        
    def initialize(self) -> None:
        """Skip Kafka initialization"""
        pass
        
    def produce(self, key: str, value: Any) -> None:
        """Record the produced ack instead of sending to Kafka"""
        self.produced_acks.append(value)

    # Override the async method with a sync version for testing
    def generate_and_produce(self, **kwargs) -> None:
        """Synchronous version for testing"""
        # Create a new order or update an existing one
        if not self.orders or random.random() < 0.7:  # 70% chance to create new order
            # Generate new order Ack using the AckDataGenerator
            generator = AckDataGenerator()
            ack = generator.generate(status="open")
            
            # Store order
            self.orders[ack.order_id] = {
                "status": OrderStatus.OPEN,
                "amount": ack.amount,
                "filled": 0,
                "direction": ack.direction,
                "client_order_id": ack.client_order_id
            }
            
            # Produce to Kafka
            self.produce(ack.order_id, ack)
        else:
            # Update an existing order
            if not self.orders:
                return
                
            # Choose a random existing order
            order_id = random.choice(list(self.orders.keys()))
            order = self.orders[order_id]
            
            # Determine next status based on current status
            if order["status"] == OrderStatus.OPEN:
                if random.random() < 0.3:  # 30% chance to cancel
                    new_status = OrderStatus.CANCELLED
                    change_reason = "cancel"
                    delete_reason = random.choice(["client_cancel", "session_end"])
                    filled = 0
                    remaining = 0
                else:  # 70% chance to partially fill
                    new_status = OrderStatus.PARTIALLY_FILLED
                    change_reason = "fill"
                    delete_reason = None
                    filled = order["amount"] * random.uniform(0.1, 0.5)
                    remaining = order["amount"] - filled
                    order["filled"] = filled
            elif order["status"] == OrderStatus.PARTIALLY_FILLED:
                if random.random() < 0.3:  # 30% chance to cancel
                    new_status = OrderStatus.CANCELLED_PARTIALLY_FILLED
                    change_reason = "cancel"
                    delete_reason = random.choice(["client_cancel", "session_end"])
                    filled = order["filled"]
                    remaining = 0
                else:  # 70% chance to fill completely
                    new_status = OrderStatus.FILLED
                    change_reason = "fill"
                    delete_reason = None
                    filled = order["amount"]
                    remaining = 0
            else:
                # Order already in terminal state, skip update
                return
            
            # Update order state
            order["status"] = new_status
            
            # Generate Ack update
            ack = ThalexAck(
                order_id=order_id,
                client_order_id=order["client_order_id"],
                instrument_name="BTC-PERPETUAL",
                direction=order["direction"],
                price=random.uniform(95000, 97000),
                amount=order["amount"],
                filled_amount=filled,
                remaining_amount=remaining,
                status=new_status,
                order_type=OrderType.LIMIT,
                time_in_force=TimeInForce.GOOD_TILL_CANCELLED,
                change_reason=change_reason,
                delete_reason=delete_reason,
                insert_reason=None if change_reason != "insert" else "client_request",
                create_time=kwargs.get("timestamp", random.uniform(1700000000, 1800000000)),
                persistent=False
            )
            
            # Produce to Kafka
            self.produce(ack.order_id, ack)
            
    def update_order(self, order_id, new_status):
        """Update a specific order to a new status"""
        if order_id not in self.orders:
            raise ValueError(f"Order ID {order_id} not found")
            
        order = self.orders[order_id]
        
        # Determine the appropriate state based on new status
        if new_status == OrderStatus.CANCELLED:
            change_reason = "cancel"
            delete_reason = "client_cancel"
            filled = 0
            remaining = 0
        elif new_status == OrderStatus.PARTIALLY_FILLED:
            change_reason = "fill"
            delete_reason = None
            filled = order["amount"] * 0.5  # Always 50% filled for testing
            remaining = order["amount"] - filled
            order["filled"] = filled
        elif new_status == OrderStatus.FILLED:
            change_reason = "fill"
            delete_reason = None
            filled = order["amount"]
            remaining = 0
        elif new_status == OrderStatus.CANCELLED_PARTIALLY_FILLED:
            change_reason = "cancel"
            delete_reason = "client_cancel"
            filled = order["filled"]
            remaining = 0
        else:
            raise ValueError(f"Unsupported status transition to {new_status}")
        
        # Update order state
        order["status"] = new_status
        
        # Generate Ack update
        ack = ThalexAck(
            order_id=order_id,
            client_order_id=order["client_order_id"],
            instrument_name="BTC-PERPETUAL",
            direction=order["direction"],
            price=96000.0,  # Fixed price for testing
            amount=order["amount"],
            filled_amount=filled,
            remaining_amount=remaining,
            status=new_status,
            order_type=OrderType.LIMIT,
            time_in_force=TimeInForce.GOOD_TILL_CANCELLED,
            change_reason=change_reason,
            delete_reason=delete_reason,
            insert_reason=None,
            create_time=datetime.datetime.now().timestamp(),
            persistent=False
        )
        
        # Produce to Kafka
        self.produce(ack.order_id, ack)
        
        return ack


class MockTradeProducer(TradeProducer):
    """Mock TradeProducer that doesn't connect to Kafka"""
    
    def __init__(self):
        """Initialize with mock config and don't connect to Kafka"""
        super().__init__(MockConfig.get_config())
        self.produced_trades: List[ThalexTrade] = []
        
    def initialize(self) -> None:
        """Skip Kafka initialization"""
        pass
        
    def produce(self, key: str, value: Any) -> None:
        """Record the produced trade instead of sending to Kafka"""
        self.produced_trades.append(value)
        
    # Override the async method with a sync version for testing
    def generate_and_produce(self, **kwargs) -> None:
        """Synchronous version for testing"""
        # Decide whether to use an existing order ID or create a new one
        order_id = None
        if self.order_ids and random.random() < 0.3:  # 30% chance to use existing order
            order_id = random.choice(list(self.order_ids))
        
        # Generate a trade using the TradeDataGenerator
        generator = TradeDataGenerator()
        # If order_id is None, the generator will create a new one
        trade = generator.generate(order_id=order_id if order_id else f"order_{random.randint(10000, 99999)}")
        
        # Add order_id to tracked orders
        self.order_ids.add(trade.order_id)
        
        # Limit the number of tracked order IDs to prevent memory issues
        if len(self.order_ids) > 100:
            # Remove a random subset of orders
            to_remove = random.sample(list(self.order_ids), 20)
            for order_id in to_remove:
                self.order_ids.remove(order_id)
        
        # Produce to Kafka
        self.produce(trade.trade_id, trade)


class MockTickerProducer(TickerProducer):
    """Mock TickerProducer that doesn't connect to Kafka"""
    
    def __init__(self):
        """Initialize with mock config and don't connect to Kafka"""
        super().__init__(MockConfig.get_config())
        self.produced_tickers: List[ThalexTicker] = []
        
    def initialize(self) -> None:
        """Skip Kafka initialization"""
        pass
        
    def produce(self, key: str, value: Any) -> None:
        """Record the produced ticker instead of sending to Kafka"""
        self.produced_tickers.append(value)
        
    # Override the async method with a sync version for testing
    def generate_and_produce(self, **kwargs) -> None:
        """Synchronous version for testing"""
        # Generate ticker data using the TickerDataGenerator
        generator = TickerDataGenerator()
        ticker = generator.generate()
        
        # Produce to Kafka
        self.produce(ticker.instrument_name, ticker)


class AckProducerTest(unittest.TestCase):
    """Test AckProducer business logic"""
    
    def test_generate_new_order(self):
        """Test generation of new orders"""
        # Create producer
        producer = MockAckProducer()
        
        # Generate and produce
        producer.generate_and_produce()
        
        # Check we produced something
        self.assertEqual(len(producer.produced_acks), 1)
        
        # Check it's a valid ack with expected status
        ack = producer.produced_acks[0]
        self.assertEqual(ack.status, OrderStatus.OPEN)
        self.assertEqual(ack.change_reason, "insert")
        self.assertIsNotNone(ack.insert_reason)
        self.assertIsNone(ack.delete_reason)
        self.assertEqual(ack.filled_amount, 0.0)
        self.assertEqual(ack.remaining_amount, ack.amount)
    
    def test_order_update_flow(self):
        """Test order update flow with different status transitions"""
        # Create producer
        producer = MockAckProducer()
        
        # First create a new order
        producer.generate_and_produce()
        
        # Save its ID
        order_id = producer.produced_acks[0].order_id
        initial_amount = producer.produced_acks[0].amount
        
        # Use the direct update method to cancel the order
        producer.update_order(order_id, OrderStatus.CANCELLED)
        
        # Now check the update
        self.assertEqual(len(producer.produced_acks), 2)
        update = producer.produced_acks[1]
        
        # Verify it's an update to our order
        self.assertEqual(update.order_id, order_id)
        
        # Should be cancelled now
        self.assertEqual(update.status, OrderStatus.CANCELLED)
        self.assertEqual(update.change_reason, "cancel")
        self.assertIsNotNone(update.delete_reason)
        self.assertEqual(update.filled_amount, 0.0)
        self.assertEqual(update.remaining_amount, 0.0)


class TradeProducerTest(unittest.TestCase):
    """Test TradeProducer business logic"""
    
    def test_trade_generation(self):
        """Test trade generation"""
        # Create producer
        producer = MockTradeProducer()
        
        # Generate and produce
        producer.generate_and_produce()
        
        # Check we produced something
        self.assertEqual(len(producer.produced_trades), 1)
        
        # Check it's a valid trade
        trade = producer.produced_trades[0]
        self.assertIsNotNone(trade.trade_id)
        self.assertIsNotNone(trade.order_id)
        self.assertIsNotNone(trade.instrument_name)
        self.assertIsNotNone(trade.price)
        self.assertIsNotNone(trade.amount)
        self.assertIsNotNone(trade.maker_taker)
        self.assertIsNotNone(trade.time)
    
    def test_reuse_order_id(self):
        """Test reuse of order ID for multiple trades"""
        # Create producer
        producer = MockTradeProducer()
        
        # First create a trade
        producer.generate_and_produce()
        
        # Save its order ID
        order_id = producer.produced_trades[0].order_id
        
        # Now patch random to ensure we reuse this order ID
        original_choice = random.choice
        random.choice = lambda seq: order_id if isinstance(seq, list) and order_id in seq else original_choice(seq)
        original_random = random.random
        random.random = lambda: 0.0  # Always return 0, which is < 0.3 = reuse order
        
        try:
            # Generate another trade
            producer.generate_and_produce()
            
            # Check we produced a second trade
            self.assertEqual(len(producer.produced_trades), 2)
            
            # Check it reuses the order ID
            self.assertEqual(producer.produced_trades[1].order_id, order_id)
            
        finally:
            # Restore original random functions
            random.choice = original_choice
            random.random = original_random


class TickerProducerTest(unittest.TestCase):
    """Test TickerProducer business logic"""
    
    def test_ticker_generation(self):
        """Test ticker generation"""
        # Create producer
        producer = MockTickerProducer()
        
        # Generate and produce
        producer.generate_and_produce()
        
        # Check we produced something
        self.assertEqual(len(producer.produced_tickers), 1)
        
        # Check it's a valid ticker
        ticker = producer.produced_tickers[0]
        self.assertEqual(ticker.instrument_name, "BTC-PERPETUAL")
        self.assertIsNotNone(ticker.mark_price)
        self.assertIsNotNone(ticker.mark_timestamp)
        self.assertIsNotNone(ticker.best_bid_price)
        self.assertIsNotNone(ticker.best_ask_price)


if __name__ == '__main__':
    unittest.main()
