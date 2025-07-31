"""
Ack sink connector for CrypticsLabBot.
Configures JDBC sink connector for Ack data.
"""

import logging
from typing import Any, Dict

from python_pipeline.consumers.base_sink_connector import BaseSinkConnector
from python_pipeline.models.ack import ThalexAck

# Configure logging
logger = logging.getLogger(__name__)


class AckSinkConnector(BaseSinkConnector):
    """PostgreSQL sink connector for ack data"""
    
    topic_key: str = "ack"
    connector_name_prefix: str = "postgres-sink"
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the ack sink connector.
        
        Args:
            config: Application configuration
        """
        super().__init__(
            config=config,
            model_class=ThalexAck,
            table_name="ack_data",
            primary_keys=["id"]
        )

