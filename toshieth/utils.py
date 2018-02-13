from toshi.utils import parse_int
from toshi.ethereum.utils import data_decoder
from toshi.ethereum.tx import (
    create_transaction, add_signature_to_transaction
)

class RedisLockException(Exception):
    pass

class RedisLock:
    def __init__(self, redis, key, raise_when_locked=None, prefix="lock:", ex=30):
        self.redis = redis
        self.key = prefix + key
        self.raise_when_locked = raise_when_locked or RedisLockException
        self.ex = ex
        self.locked = None

    def __enter__(self):
        self.locked = locked = self.redis.set(self.key, True, nx=True, ex=self.ex)
        if not locked:
            raise self.raise_when_locked()

    def __exit__(self, exc_type, exc_value, traceback):
        if self.locked:
            self.redis.delete(self.key)

def database_transaction_to_rlp_transaction(transaction):
    """returns an rlp transaction for the given transaction"""

    nonce = transaction['nonce']
    value = parse_int(transaction['value'])
    gas = parse_int(transaction['gas'])
    gas_price = parse_int(transaction['gas_price'])

    tx = create_transaction(nonce=nonce, gasprice=gas_price, startgas=gas,
                            to=transaction['to_address'], value=value,
                            data=data_decoder(transaction['data']),
                            v=parse_int(transaction['v']),
                            r=parse_int(transaction['r']),
                            s=parse_int(transaction['s']))

    return tx
