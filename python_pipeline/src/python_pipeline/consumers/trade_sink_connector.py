"""
Trade sink connector for CrypticsLabBot.
Configures JDBC sink connector for Trade data.
"""

import logging
from typing import Any, Dict

from python_pipeline.consumers.base_sink_connector import BaseSinkConnector
from python_pipeline.models.trade import ThalexTrade

# Configure logging
logger = logging.getLogger(__name__)


class TradeSinkConnector(BaseSinkConnector):
    """PostgreSQL sink connector for trade data"""
    
    topic_key: str = "trade"
    connector_name_prefix: str = "postgres-sink"
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the trade sink connector.
        
        Args:
            config: Application configuration
        """
        super().__init__(
            config=config,
            model_class=ThalexTrade,
            table_name="trade_data",
            primary_keys=["id"]
        )

