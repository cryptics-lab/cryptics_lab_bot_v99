"""
Order data sink connector for PostgreSQL
"""

import logging
from typing import Any, Dict

from python_pipeline.consumers.base_sink_connector import BaseSinkConnector
from python_pipeline.models import ThalexOrder

# Configure logging
logger = logging.getLogger(__name__)


class OrderSinkConnector(BaseSinkConnector):
    """PostgreSQL sink connector for order data"""
    
    topic_key: str = "order"
    connector_name_prefix: str = "postgres-sink"
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the order sink connector.
        
        Args:
            config: Application configuration
        """
        super().__init__(
            config=config,
            model_class=ThalexOrder,
            table_name="order_data",
            primary_keys=["order_id", "create_time"]
        )
    
    # No need for timestamp transformations as we're now using DOUBLE PRECISION directly
