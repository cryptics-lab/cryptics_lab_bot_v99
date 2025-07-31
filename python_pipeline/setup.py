"""
This setup.py file is provided for backwards compatibility with tools
that don't support pyproject.toml yet.

For most cases, please use 'uv install .' or 'pip install -e .'
"""
from setuptools import setup

setup(
    name="python_pipeline",
    # All actual configuration is in pyproject.toml
)
