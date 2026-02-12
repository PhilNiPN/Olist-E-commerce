""" 
Centralized logging configuration for the Olist data pipeline.
"""

import logging
import sys
from pythonjsonlogger import jsonlogger


def setup_logging(level: str = 'INFO'):
    """ setup structured json logging for the entire app. """
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    logger.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    formatter = jsonlogger.JsonFormatter(
        fmt='%(asctime)s %(name)s %(levelname)s %(message)s',
        rename_fields={'asctime:': 'timestamp', 'levelname': 'level', 'name': 'logger'},
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

