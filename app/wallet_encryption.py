import base64
import itertools
import time

import requests
import tronpy
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from .config import config
from .logging import logger


class EncryptionNotSet(Exception):
    pass


class EncryptionKeyNotSet(Exception):
    pass


class EncryptionModeMismatch(SystemExit):
    pass


class wallet_encryption:

    encryption = None
    key = None

    @classmethod
    def encrypt(cls, cleartext):
        return cleartext if cls._is_noop() else cls._encrypt(cleartext)

    @classmethod
    def decrypt(cls, ciphertext):
        return ciphertext if cls._is_noop() else cls._decrypt(ciphertext)

    @classmethod
    def encrypt_db(cls):
        from .db import query_db2

        rows = query_db2("SELECT * FROM keys")
        for row in rows:
            try:
                tronpy.keys.PrivateKey(bytes.fromhex(row["private"]))
                # encrypting
                encrypted_private_key = cls.encrypt(row["private"])
                query_db2(
                    "UPDATE keys SET private = ? WHERE public = ?",
                    (encrypted_private_key, row["public"]),
                )
                logger.info(f"{row['public']} account encrypted")
            except (ValueError, tronpy.exceptions.BadKey) as e:
                logger.info(
                    f"{row['public']} is either already encrypted or private key is garbage"
                )
                continue

    @classmethod
    def setup_encryption(cls):
        cls._fetch_encryption_settings()
        cls._validate_encryption_settings()

    @classmethod
    def _fetch_encryption_settings(cls):
        """Fetches encryption settings from Shkeeper"""

        for symbol in itertools.cycle(
            ["TRX"] + [token.symbol for token in config.get_tokens()]
        ):
            try:
                res = requests.get(
                    f"http://{config.SHKEEPER_HOST}/api/v1/{symbol}/decrypt",
                    headers={"X-Shkeeper-Backend-Key": config.SHKEEPER_BACKEND_KEY},
                ).json()

                if res["persistent_status"] == "disabled":
                    cls.encryption = False
                    break

                if res["persistent_status"] == "enabled":
                    if res["runtime_status"] == "success":
                        cls.encryption = True
                        cls.key = cls._get_key_from_password(res["key"])
                        logger.info(
                            "Wallet encryption is enabled, encryption key is set!"
                        )
                        break
                    else:
                        logger.info("Waiting for encryption key...")

                if res["persistent_status"] == "pending":
                    logger.info("Waiting for encryption setting...")

            except KeyError as e:
                logger.warning(f"Error getting encryption password from {res!r}: {e!r}")
            except Exception as e:
                logger.info(f"Error getting encryption password: {e!r}")

            finally:
                time.sleep(5)

    @classmethod
    def _validate_encryption_settings(cls):
        """Compares encryption runtime settings to wallet encryption"""

        from .db import query_db2

        db_encrypted = None
        if table_exists := query_db2(
            "SELECT * FROM sqlite_master WHERE type='table' AND name='keys'", one=True
        ):
            if first_key := query_db2("SELECT private FROM keys LIMIT 1", one=True):
                try:
                    tronpy.keys.PrivateKey(bytes.fromhex(first_key["private"]))
                    db_encrypted = False
                except (ValueError, tronpy.exceptions.BadKey) as e:
                    db_encrypted = True

        if db_encrypted is None:
            logger.info(
                "Wallet is not initialized yet. Skipping encryption validation."
            )
            return None

        if (
            cls.encryption is True
            and db_encrypted is False
            and config.get("FORCE_WALLET_ENCRYPTION")
        ):
            logger.info(
                "DB is unencrypted, force wallet encryption is requested by env"
            )
            cls.encrypt_db()
            return True

        else:
            if cls.encryption is db_encrypted:
                logger.info("Encryption settings are valid.")
                return True
            else:
                raise EncryptionModeMismatch(
                    f"Shkeeper requested encryption={cls.encryption} but "
                    f"wallet is in encryption={db_encrypted} mode. "
                    "Exiting to prevent data corruption."
                )

    @classmethod
    def _get_key_from_password(cls, password: str):
        salt = b"Shkeeper4TheWin!"
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=500_000,
        )
        return base64.urlsafe_b64encode(kdf.derive(password.encode()))

    @classmethod
    def _is_noop(cls):
        "Checks if encryption/decryption should be no-op."

        if cls.encryption is None:
            raise EncryptionNotSet("Waiting for encryption status")
        if cls.encryption is False:
            return True
        else:
            if cls.key is None:
                raise EncryptionKeyNotSet("Waiting for encryption key")
            return False

    @classmethod
    def _encrypt(cls, cleartext: str):
        return base64.urlsafe_b64encode(
            Fernet(cls.key).encrypt(cleartext.encode())
        ).decode()

    @classmethod
    def _decrypt(cls, ciphertext: str):
        return Fernet(cls.key).decrypt(base64.urlsafe_b64decode(ciphertext)).decode()
