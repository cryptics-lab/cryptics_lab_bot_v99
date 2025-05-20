"""
Factory for creating data generators.
"""

from typing import Dict, Type

from python_pipeline.data_generators.ack_generator import AckDataGenerator
from python_pipeline.data_generators.base_generator import BaseDataGenerator
from python_pipeline.data_generators.ticker_generator import TickerGenerator
from python_pipeline.data_generators.trade_generator import TradeGenerator

# Import other generators as they're created

class DataGeneratorFactory:
    """Factory for creating data generators"""
    
    # Registry of available generators
    _generators = {
        "ack": AckDataGenerator,
        "ticker": TickerGenerator,
        "trade": TradeGenerator,
        # Add more generators as they're implemented
    }
    
    @classmethod
    def create(cls, model_name: str) -> BaseDataGenerator:
        """
        Create a data generator for the specified model
        
        Args:
            model_name: Name of the model
            
        Returns:
            Instance of an appropriate data generator
            
        Raises:
            ValueError: If no generator exists for the model
        """
        if model_name not in cls._generators:
            raise ValueError(f"No data generator available for model: {model_name}")
            
        generator_class = cls._generators[model_name]
        return generator_class()
