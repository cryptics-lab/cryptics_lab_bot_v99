"""
Base abstract model for CrypticsLabBot data models.
Defines the interface for all model implementations.
"""

from abc import ABC
from typing import Any, ClassVar

from pydantic import BaseModel


class ModelBase(BaseModel, ABC):
    """Base class for all data models"""
    
    # Class variables to be defined by subclasses
    model_name: ClassVar[str]

