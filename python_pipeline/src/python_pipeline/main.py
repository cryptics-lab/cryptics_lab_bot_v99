#!/usr/bin/env python3
"""
CrypticsLabBot Pipeline
=======================
Main entry point for the pipeline using modular services
"""

import asyncio
import logging
import os
import sys
from pathlib import Path

from python_pipeline.services import PipelineOrchestrator
from python_pipeline.utils.health_check import start_health_server

# Add the project root to Python path
sys.path.append(str(Path(__file__).parent.parent))

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger: logging.Logger = logging.getLogger("cryptics-pipeline")


async def main() -> None:
    """Main entry point"""
    logger.info("Starting CrypticsLabBot Pipeline")
    
    # Start health check server if running in Docker
    if os.environ.get('python_running_in_docker', '').lower() == 'true':
        start_health_server()
        logger.info("Health check server started")
    
    orchestrator: PipelineOrchestrator = PipelineOrchestrator()
    success: bool = await orchestrator.run()
    
    if success:
        logger.info("Pipeline setup completed successfully")
    else:
        logger.error("Pipeline setup failed")
        sys.exit(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
