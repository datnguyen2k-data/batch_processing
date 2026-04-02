import logging
import sys
from pythonjsonlogger import jsonlogger

def get_logger(name: str) -> logging.Logger:
    """Setup and return a JSON formatted logger for stdout."""
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        logHandler = logging.StreamHandler(sys.stdout)
        
        # Configure JSON formatter for Datadog/ELK parsing
        formatter = jsonlogger.JsonFormatter(
            fmt='%(asctime)s %(levelname)s %(name)s %(message)s',
            datefmt='%Y-%m-%dT%H:%M:%S%z'
        )
        logHandler.setFormatter(formatter)
        
        logger.addHandler(logHandler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
        
    return logger
