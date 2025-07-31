"""
Ticker data sink connector for PostgreSQL
"""

import logging
from typing import Any, Dict

from python_pipeline.consumers.base_sink_connector import BaseSinkConnector
from python_pipeline.models.ticker import ThalexTicker

# Configure logging
logger = logging.getLogger(__name__)


class TickerSinkConnector(BaseSinkConnector):
    """PostgreSQL sink connector for ticker data"""
    
    topic_key: str = "ticker"
    connector_name_prefix: str = "postgres-sink"
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the ticker sink connector.
        
        Args:
            config: Application configuration
        """
        super().__init__(
            config=config,
            model_class=ThalexTicker,
            table_name="ticker_data",
            primary_keys=["id"]
        )

