import logging

from .config import config

logger = logging.getLogger(__name__)
if config['DEBUG']:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
