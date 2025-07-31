"""
Trade data generator for CrypticsLabBot.
"""

import datetime
import random
import uuid

from python_pipeline.data_generators.base_generator import BaseDataGenerator
from python_pipeline.models.trade import ThalexTrade, MakerTakerRole


class TradeGenerator(BaseDataGenerator):
    """Generator for Thalex trade data"""
    
    def generate(self, **kwargs) -> ThalexTrade:
        """Generate sample trade data based on Thalex format"""
        
        # Generate a random trade ID if not provided
        trade_id = kwargs.get('trade_id', f"TRADE-{str(uuid.uuid4())[:8]}")
        
        # Generate order ID if not provided
        order_id = kwargs.get('order_id')
        if not order_id:
            order_id = f"{random.randint(0, 255):02X}" + f"{random.randint(0, 16777215):06X}" + "00000000"
        
        # Optional client order ID
        client_order_id = kwargs.get('client_order_id', random.randint(100, 999) if random.random() > 0.3 else None)
        
        # Set instrument name
        instrument_name = kwargs.get('instrument_name', "BTC-PERPETUAL")
        
        # Set price and amount
        price = kwargs.get('price', random.uniform(95000, 97000))
        amount = kwargs.get('amount', random.uniform(0.1, 1.0))
        
        # Set maker/taker role
        if 'maker_taker' in kwargs:
            # Use the provided maker_taker (string or enum)
            maker_taker_val = kwargs['maker_taker']
            if isinstance(maker_taker_val, str):
                maker_taker = MakerTakerRole(maker_taker_val)
            else:
                maker_taker = maker_taker_val
        else:
            # Select a random maker_taker from the enum
            maker_taker = random.choice(list(MakerTakerRole))
        
        # Set timestamp
        time = kwargs.get('time', datetime.datetime.now().timestamp())
        
        return ThalexTrade(
            trade_id=trade_id,
            order_id=order_id,
            client_order_id=client_order_id,
            instrument_name=instrument_name,
            price=price,
            amount=amount,
            maker_taker=maker_taker,
            time=time
        )
