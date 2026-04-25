"""Shared utils for data pipelines."""
import logging
import sys
import time
from contextlib import contextmanager
from typing import Generator, Optional

def configure_logging(level: int = logging.INFO, log_format: Optional[str] = None) -> logging.Logger:
    
    """
    Configure standardized logging for pipeline execution.
    
    Args:
        level: Logging level (default: INFO)
        log_format: Custom format string (optional)
        
    Returns:
        Configured logger instance
    """

    if log_format is None:
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    logging.basicConfig(
    level=level,
    format=log_format,
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True
    )
    
    return logging.getLogger(__name__)

@contextmanager
def step_timer(name: str, logger: Optional[logging.Logger] = None) -> Generator[None, None, None]:
    """
    Context manager for timing and logging execution steps.
    
    This context manager automatically logs the start and completion time
    of a code block, providing visibility into pipeline performance.
    
    Args:
        name: Description of the step being timed
        logger: Optional logger instance (creates one if not provided)
    Yields:
        None
        
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    start = time.perf_counter()
    logger.info("Starting: %s", name)
    
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        logger.info("Completed: %s (%.2f seconds)", name, elapsed)