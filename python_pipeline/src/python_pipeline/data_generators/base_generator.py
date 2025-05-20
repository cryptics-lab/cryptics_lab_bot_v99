"""
Base data generator for CrypticsLabBot.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Type

from python_pipeline.models.model_base import ModelBase


class BaseDataGenerator(ABC):
    """Base class for data generators"""
    
    @abstractmethod
    def generate(self, **kwargs) -> ModelBase:
        """Generate fake data for the model"""
        pass
