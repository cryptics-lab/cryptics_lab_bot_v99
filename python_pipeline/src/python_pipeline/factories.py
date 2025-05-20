"""
Factory classes for the CrypticsLabBot pipeline.
Provides factories for creating producers and connectors.
"""

import logging
from typing import Any, Dict, Type

from python_pipeline.consumers.ack_sink_connector import AckSinkConnector
from python_pipeline.consumers.base_sink_connector import BaseSinkConnector
from python_pipeline.consumers.index_sink_connector import IndexSinkConnector
from python_pipeline.consumers.ticker_sink_connector import TickerSinkConnector
from python_pipeline.consumers.trade_sink_connector import TradeSinkConnector
from python_pipeline.producers.ack_producer import AckProducer
from python_pipeline.producers.base_producer import AvroProducerBase
from python_pipeline.producers.index_producer import IndexProducer
from python_pipeline.producers.ticker_producer import TickerProducer
from python_pipeline.producers.trade_producer import TradeProducer

# Configure logging
logger = logging.getLogger(__name__)


class ProducerFactory:
    """Factory for creating Avro producers"""
    
    # Map of producer types to producer classes
    _producers = {
        "ticker": TickerProducer,
        "ack": AckProducer,
        "trade": TradeProducer,
        "index": IndexProducer
    }
    
    @classmethod
    def register(cls, name: str, producer_class: Type[AvroProducerBase]) -> None:
        """
        Register a producer class
        
        Args:
            name: Name of the producer
            producer_class: Producer class to register
        """
        cls._producers[name] = producer_class
        logger.info(f"Registered producer: {name}")
    
    @classmethod
    def create(cls, name: str, config: Dict[str, Any]) -> AvroProducerBase:
        """
        Create a producer instance
        
        Args:
            name: Name of the producer
            config: Configuration dictionary
            
        Returns:
            Producer instance
        """
        if name not in cls._producers:
            raise ValueError(f"Unknown producer type: {name}")
        
        return cls._producers[name](config)


class ConnectorFactory:
    """Factory for creating sink connectors"""
    
    # Map of connector types to connector classes
    _connectors = {
        "ticker": TickerSinkConnector,
        "ack": AckSinkConnector,
        "trade": TradeSinkConnector,
        "index": IndexSinkConnector
    }
    
    @classmethod
    def register(cls, name: str, connector_class: Type[BaseSinkConnector]) -> None:
        """
        Register a connector class
        
        Args:
            name: Name of the connector
            connector_class: Connector class to register
        """
        cls._connectors[name] = connector_class
        logger.info(f"Registered connector: {name}")
    
    @classmethod
    def create(cls, name: str, config: Dict[str, Any]) -> BaseSinkConnector:
        """
        Create a connector instance
        
        Args:
            name: Name of the connector
            config: Configuration dictionary
            
        Returns:
            Connector instance
        """
        if name not in cls._connectors:
            raise ValueError(f"Unknown connector type: {name}")
        
        return cls._connectors[name](config)
