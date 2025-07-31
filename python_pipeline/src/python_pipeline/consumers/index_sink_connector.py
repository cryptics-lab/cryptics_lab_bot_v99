"""
Index data sink connector for PostgreSQL
"""

import logging
from typing import Any, Dict

from python_pipeline.consumers.base_sink_connector import BaseSinkConnector
from python_pipeline.models.index import PriceIndex

# Configure logging
logger = logging.getLogger(__name__)


class IndexSinkConnector(BaseSinkConnector):
    """PostgreSQL sink connector for index data"""
    
    topic_key: str = "index"
    connector_name_prefix: str = "postgres-sink"
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the index sink connector.
        
        Args:
            config: Application configuration
        """
        super().__init__(
            config=config,
            model_class=PriceIndex,
            table_name="index_data",
            primary_keys=["id"]
        )

