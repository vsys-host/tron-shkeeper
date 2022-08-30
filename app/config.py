import os
from decimal import Decimal

config = {

    'DEBUG': os.environ.get('DEBUG', False),
    'DATABASE': os.environ.get('DATABASE', 'data/database.db'),

    'REDIS_HOST': os.environ.get('REDIS_HOST', 'localhost'),
    'EVENT_SERVER_HOST': os.environ.get('EVENT_SERVER_HOST', 'localhost:5001'),
    'FULLNODE_URL': os.environ.get('FULLNODE_URL', 'http://62.182.80.10:8090'),
    'SOLIDITYNODE_URL': os.environ.get('SOLIDITYNODE_URL', 'http://62.182.80.10:8091'),

    'API_USERNAME': os.environ.get('BTC_USERNAME', 'shkeeper'),
    'API_PASSWORD': os.environ.get('BTC_PASSWORD', 'shkeeper'),
    'SHKEEPER_KEY': os.environ.get('SHKEEPER_BACKEND_KEY', 'shkeeper'),
    'SHKEEPER_HOST': os.environ.get('SHKEEPER_HOST', 'localhost:5000'),

    'TX_FEE': Decimal(os.environ.get('TX_FEE', 10)),
    'TX_FEE_LIMIT': Decimal(os.environ.get('TX_FEE_LIMIT', 5)),

    'TOKENS': {
        'USDT': {'contract_address': 'TXLAQ63Xg1NAzckPwKHvzw7CSEmLMEqcdj'},
    },

}
