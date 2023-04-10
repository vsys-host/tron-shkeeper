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
    'FULLNODE_URL': os.environ.get('FULLNODE_URL', 'http://fullnode.tron.shkeeper.io'),
    'SOLIDITYNODE_URL': os.environ.get('SOLIDITYNODE_URL', 'http://soliditynode.tron.shkeeper.io'),
    'TRON_NODE_USERNAME': os.environ.get('TRON_NODE_USERNAME', 'shkeeper'),
    'TRON_NODE_PASSWORD': os.environ.get('TRON_NODE_PASSWORD', 'tron'),

    'API_USERNAME': os.environ.get('BTC_USERNAME', 'shkeeper'),
    'API_PASSWORD': os.environ.get('BTC_PASSWORD', 'shkeeper'),
    'SHKEEPER_KEY': os.environ.get('SHKEEPER_BACKEND_KEY', 'shkeeper'),
    'SHKEEPER_HOST': os.environ.get('SHKEEPER_HOST', 'localhost:5000'),

    'TX_FEE': Decimal(os.environ.get('TX_FEE', 40)),  # includes bandwidth, energy and activation fees
    'TX_FEE_LIMIT': Decimal(os.environ.get('TX_FEE_LIMIT', 50)),  # max TRX tx can burn for resources (energy, bandwidth)

    # Block scanner
    'BLOCK_SCANNER_STATS_LOG_PERIOD': int(os.environ.get('BLOCK_SCANNER_STATS_LOG_PERIOD', 5)),
    'BLOCK_SCANNER_MAX_BLOCK_CHUNK_SIZE': int(os.environ.get('BLOCK_SCANNER_MAX_BLOCK_CHUNK_SIZE', 10)),
    'BLOCK_SCANNER_INTERVAL_TIME': int(os.environ.get('BLOCK_SCANNER_INTERVAL_TIME', 3)),
    'BLOCK_SCANNER_LAST_BLOCK_NUM_HINT': os.environ.get('BLOCK_SCANNER_LAST_BLOCK_NUM_HINT'),

    # Connection manager
    'MULTISERVER_CONFIG_JSON': os.environ.get('MULTISERVER_CONFIG_JSON'),
    'MULTISERVER_REFRESH_BEST_SERVER_PERIOD': int(os.environ.get('MULTISERVER_REFRESH_BEST_SERVER_PERIOD', 20)),

    'TOKENS': {
        'main': {
            'USDT': {'contract_address': 'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t'},
            'USDC': {'contract_address': 'TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8'},
        },
        'nile': {
            'USDT': {'contract_address': 'TXLAQ63Xg1NAzckPwKHvzw7CSEmLMEqcdj'},
        },
    },
}

def get_contract_address(symbol):
    return config['TOKENS'][config['TRON_NETWORK']][symbol]['contract_address']

def get_min_transfer_threshold(symbol):
    return config['TOKENS'][config['TRON_NETWORK']][symbol].get('min_transfer_threshold', Decimal('0.5'))

def get_symbol(contract_address):
    cont_addr_to_symbol = {
        config['TOKENS'][config['TRON_NETWORK']][symbol]['contract_address']: symbol
            for symbol in config['TOKENS'][config['TRON_NETWORK']]
    }
    return cont_addr_to_symbol[contract_address]