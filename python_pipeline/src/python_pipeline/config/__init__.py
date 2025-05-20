"""
Configuration package for CrypticsLabBot.
"""

from python_pipeline.config.settings import settings


def get_config():
    """Get configuration as dictionary for backward compatibility"""
    return settings.dict()

__all__ = ['get_config', 'settings']
