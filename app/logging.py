import logging

from .config import config


log_format = (
    "%(levelname)s %(filename)s:%(lineno)s %(threadName)s %(funcName)s(): %(message)s"
)

logger = logging.getLogger(__name__)
if config.DEBUG:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
formatter = logging.Formatter(log_format)
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.propagate = False

from flask import logging as flog

flog.default_handler.setFormatter(logging.Formatter(log_format))
