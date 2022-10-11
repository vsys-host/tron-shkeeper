import os
from decimal import Decimal

config = {

    'TRON_NETWORK': os.environ.get('TRON_NETWORK', 'main'),  # main, nile
    'DEBUG': os.environ.get('DEBUG', False),
    'LOGGING_LEVEL': os.environ.get('LOGGING_LEVEL', 'INFO'),
    'DATABASE': os.environ.get('DATABASE', 'data/database.db'),
    'BALANCES_DATABASE': os.environ.get('BALANCES_DATABASE', 'data/trc20balances.db'),
    'CONCURRENT_MAX_WORKERS': int(os.environ.get('CONCURRENT_MAX_WORKERS', 15)),
    'CONCURRENT_MAX_RETRIES': int(os.environ.get('CONCURRENT_MAX_RETRIES', 10)),
    'UPDATE_TOKEN_BALANCES_EVERY_SECONDS': int(os.environ.get('UPDATE_TOKEN_BALANCES_EVERY_SECONDS', 60)),

    'REDIS_HOST': os.environ.get('REDIS_HOST', 'localhost'),
    'EVENT_SERVER_HOST': os.environ.get('EVENT_SERVER_HOST', 'events.tron.shkeeper.io'),
    'FULLNODE_URL': os.environ.get('FULLNODE_URL', 'http://fullnode.tron.shkeeper.io'),
    'SOLIDITYNODE_URL': os.environ.get('SOLIDITYNODE_URL', 'http://soliditynode.tron.shkeeper.io'),
    'TRON_NODE_USERNAME': os.environ.get('TRON_NODE_USERNAME', 'shkeeper'),
    'TRON_NODE_PASSWORD': os.environ.get('TRON_NODE_PASSWORD', 'tron'),

    'API_USERNAME': os.environ.get('BTC_USERNAME', 'shkeeper'),
    'API_PASSWORD': os.environ.get('BTC_PASSWORD', 'shkeeper'),
    'SHKEEPER_KEY': os.environ.get('SHKEEPER_BACKEND_KEY', 'shkeeper'),
    'SHKEEPER_HOST': os.environ.get('SHKEEPER_HOST', 'localhost:5000'),

    'TX_FEE': Decimal(os.environ.get('TX_FEE', 15)),  # includes bandwidth, energy and activation fees
    'TX_FEE_LIMIT': Decimal(os.environ.get('TX_FEE_LIMIT', 20)),  # max TRX tx can burn for resources (energy, bandwidth)

    'TOKENS': {
        'main': {
            'USDT': {'contract_address': 'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t'},
            'USDC': {'contract_address': 'TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8'},
        },
        'nile': {
            'USDT': {'contract_address': 'TXLAQ63Xg1NAzckPwKHvzw7CSEmLMEqcdj'},
            # USDC is not on Nile testnet, so use USDT contract instead
            # good enough for testing purposes
            'USDC': {'contract_address': 'TXLAQ63Xg1NAzckPwKHvzw7CSEmLMEqcdj'},
        },
    },
}

def get_contract_address(symbol):
    return config['TOKENS'][config['TRON_NETWORK']][symbol]['contract_address']