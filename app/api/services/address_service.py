from bip_utils import Bip32PublicKey, Bip44, Bip44Coins, Bip44Changes
from Crypto.Hash import keccak
import base58
from ...db import get_db
from ...logging import logger
from ...wallet_encryption import wallet_encryption
from ...block_scanner import BlockScanner


def generate_address_from_xpub(symbol, xpub_str):
    db = get_db()
    bip44_acc = Bip44.FromExtendedKey(xpub_str, Bip44Coins.TRON)
    bip44_change = bip44_acc.Change(Bip44Changes.CHAIN_EXT)
    count = db.execute("SELECT COUNT(*) FROM keys").fetchone()[0]
    address = bip44_change.AddressIndex(count).PublicKey().ToAddress()
    db.execute(
        "INSERT INTO keys (symbol, public, private, type) VALUES (?, ?, ?, 'only_read')",
        (symbol, address, ''),
    )
    db.commit()
    BlockScanner.add_watched_account(address)
    return address


def generate_address_with_private_key(symbol, client):
    db = get_db()
    addresses = client.generate_address()
    public_address = addresses["base58check_address"]
    encrypted_priv = wallet_encryption.encrypt(addresses["private_key"])

    db.execute(
        "INSERT INTO keys (symbol, public, private, type) VALUES (?, ?, ?, 'onetime')",
        (symbol, public_address, encrypted_priv),
    )
    db.commit()
    BlockScanner.add_watched_account(public_address)

    return public_address