from pyspark.sql import SparkSession

import logging
import os

# ENV
LOG_LEVEL=os.getenv('LOG_LEVEL', 'INFO')

def log_level(level=LOG_LEVEL):
    return {
        'CRITICAL': logging.CRITICAL,
        'ERROR': logging.ERROR,
        'WARNING': logging.WARNING,
        'INFO': logging.INFO,
        'DEBUG': logging.DEBUG
    }.get(level, logging.NOTSET)

logger = logging.getLogger('default')
logger.setLevel(log_level())

base_handler = logging.StreamHandler()
base_handler.setLevel(log_level())

base_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
base_handler.setFormatter(base_formatter)

logger.addHandler(base_handler)
logger.propagate = False