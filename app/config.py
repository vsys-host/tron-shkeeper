from decimal import Decimal
from functools import cache

from pydantic import Field, Json
from pydantic_settings import BaseSettings, SettingsConfigDict

from .exceptions import UnknownToken
from .schemas import SrVote, Token, TronFullnode, TronNetwork, TronSymbol


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    TRON_NETWORK: TronNetwork = TronNetwork.mainnet
    DEBUG: bool = False
    DB_URI: str = "mysql+pymysql://root:shkeeper@mariadb/tron-shkeeper?charset=utf8mb4"
    CONCURRENT_MAX_WORKERS: int = 1
    CONCURRENT_MAX_RETRIES: int = 10
    BALANCES_RESCAN_PERIOD: int = 3600
    SCAN_ACCOUNTS_PROGRESS_LOG_INTERVAL: int = 5
    SWEEP_TRC20_RETRY_INITIAL_DELAY: int = 10
    SWEEP_TRC20_RETRY_TIMEOUT: int = 3600
    SAVE_BALANCES_TO_DB: bool = True
    REDIS_HOST: str = "localhost"
    FULLNODE_URL: str = "http://fullnode.tron.shkeeper.io"
    TRON_NODE_USERNAME: str = "shkeeper"
    TRON_NODE_PASSWORD: str = "tron"
    TRON_CLIENT_TIMEOUT: int = 10
    API_USERNAME: str = Field("shkeeper", alias="BTC_USERNAME")
    API_PASSWORD: str = Field("shkeeper", alias="BTC_PASSWORD")
    SHKEEPER_BACKEND_KEY: str = "shkeeper"
    SHKEEPER_HOST: str = "localhost:5000"
    INTERNAL_TX_FEE: Decimal = Decimal(40)
    TX_FEE: Decimal = Decimal(40)  # includes bandwidth, energy and activation fees
    TX_FEE_LIMIT: Decimal = Decimal(
        50
    )  # max TRX tx can burn for resources (energy, bandwidth)
    BANDWIDTH_PER_TRX_TRANSFER: int = 270
    BANDWIDTH_PER_DELEGE_CALL: int = 278
    BANDWIDTH_PER_UNDELEGATE_CALL: int = 280
    BANDWIDTH_PER_TRC20_TRANSFER_CALL: int = 346
    TRX_PER_BANDWIDTH_UNIT: Decimal = Decimal("0.001")
    TRX_MIN_TRANSFER_THRESHOLD: Decimal = Decimal("0.5")
    # Block scanner
    BLOCK_SCANNER_STATS_LOG_PERIOD: int = 300
    BLOCK_SCANNER_MAX_BLOCK_CHUNK_SIZE: int = 1
    BLOCK_SCANNER_INTERVAL_TIME: int = 3
    BLOCK_SCANNER_LAST_BLOCK_NUM_HINT: int | None = None
    # Connection manager
    MULTISERVER_CONFIG_JSON: Json[list[TronFullnode]] | None = None
    MULTISERVER_REFRESH_BEST_SERVER_PERIOD: int = 20
    # Account encryption
    FORCE_WALLET_ENCRYPTION: bool = False
    # DEV MODE
    DEVMODE_ENCRYPTION_PW: str | None = None
    DEVMODE_SKIP_NOTIFICATIONS: bool = False
    DEVMODE_CELERY_NODELAY: bool = False
    # Resource delegation
    ENERGY_DELEGATION_MODE: bool = False
    ENERGY_DELEGATION_MODE_ALLOW_BURN_TRX_FOR_BANDWITH: bool = False
    ENERGY_DELEGATION_MODE_ALLOW_BURN_TRX_ON_PAYOUT: bool = False
    ENERGY_DELEGATION_MODE_ALLOW_ADDITIONAL_ENERGY_DELEGATION: bool = False
    ENERGY_DELEGATION_MODE_ENERGY_DELEGATION_FACTOR: Decimal = Decimal("1.0")
    ENERGY_DELEGATION_MODE_TRC20_TRANSFER_ENERGY_ESTIMATE_OVERRIDE: int | None = None
    ENERGY_DELEGATION_MODE_SEPARATE_BALANCE_AND_ENERGY_ACCOUNTS: bool = False
    ENERGY_DELEGATION_MODE_ENERGY_ACCOUNT_PUB_KEY: str | None = None
    # Max seconds funds_sweeper will wait for delegator energy recovery before skipping the TRC20 sweep this cycle
    ENERGY_WAIT_MAX_SECONDS: int = 3600
    # Voting
    SR_VOTING: bool = False
    SR_VOTES: Json[list[SrVote]] | None = None
    SR_VOTING_ALLOW_BURN_TRX: bool = False
    # Token customization
    USDT_MIN_TRANSFER_THRESHOLD: Decimal | None = None
    USDC_MIN_TRANSFER_THRESHOLD: Decimal | None = None

    TOKENS: list[Token] = [
        Token(
            network=TronNetwork.mainnet,
            symbol=TronSymbol.USDT,
            contract_address="TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
            min_transfer_threshold="5",
            decimal=6,
        ),
        Token(
            network=TronNetwork.mainnet,
            symbol=TronSymbol.USDC,
            contract_address="TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8",
            min_transfer_threshold="5",
            decimal=6,
        ),
        Token(
            network=TronNetwork.testnet,
            symbol=TronSymbol.USDT,
            contract_address="TF17BgPaZYbz8oxbjhriubPDsA7ArKoLX3",  # JST
            min_transfer_threshold="0",
            decimal=18,
        ),
    ]

    @cache
    def get_contract_address(self, symbol):
        for token in self.TOKENS:
            if self.TRON_NETWORK is token.network and token.symbol == symbol:
                return token.contract_address
        raise UnknownToken(f"Unknown token {symbol=}")

    @cache
    def get_min_transfer_threshold(self, symbol):
        for token in self.TOKENS:
            if self.TRON_NETWORK is token.network and token.symbol == symbol:
                if hasattr(self, f"{symbol}_MIN_TRANSFER_THRESHOLD") and (
                    custom_threshold := getattr(
                        self, f"{symbol}_MIN_TRANSFER_THRESHOLD"
                    )
                ):
                    return custom_threshold
                else:
                    return token.min_transfer_threshold
        raise UnknownToken(f"Unknown token {symbol=}")

    @cache
    def get_symbol(self, contract_address):
        for token in self.TOKENS:
            if (
                self.TRON_NETWORK is token.network
                and token.contract_address == contract_address
            ):
                return token.symbol
        raise UnknownToken(f"Unknown token {contract_address=}")

    def get_decimal(self, symbol: TronSymbol) -> int:
        for token in self.TOKENS:
            if self.TRON_NETWORK is token.network and token.symbol == symbol:
                return token.decimal
        raise UnknownToken(f"Unknown token {symbol=}")

    def get_internal_trc20_tx_fee(self):
        return self.INTERNAL_TX_FEE

    @cache
    def get_tokens(self):
        return list(filter(lambda x: x.network == self.TRON_NETWORK, self.TOKENS))

    def __hash__(self):
        return hash(42)

config = Settings()
