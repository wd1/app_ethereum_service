import asyncio
from tornado.testing import gen_test

from toshieth.test.base import EthServiceBaseTest
from toshi.tasks import TaskListener, TaskHandler
from toshi.test.redis import requires_redis
from toshi.test.database import requires_database
from toshi.test.ethereum.parity import FAUCET_PRIVATE_KEY, FAUCET_ADDRESS
from toshi.ethereum.tx import create_transaction, sign_transaction, calculate_transaction_hash
from toshi.ethereum.utils import data_decoder, data_encoder, personal_sign

TEST_PRIVATE_KEY = data_decoder("0xe8f32e723decf4051aefac8e2c93c9c5b214313817cdb01a1494b917c8436b35")
TEST_ADDRESS = "0x056db290f8ba3250ca64a45d16284d04bc6f5fbf"

class MockTaskListener(TaskListener):
    def __init__(self, app):
        super().__init__([(MockTaskHandler,)], app)
        self.task_queue = asyncio.Queue()

    def get(self):
        return self.task_queue.get()

class MockTaskHandler(TaskHandler):
    def update_transaction(self, transaction_id, status):
        self.listener.task_queue.put_nowait((transaction_id, status))


class TransactionCancelTest(EthServiceBaseTest):

    @gen_test(timeout=15)
    @requires_database
    @requires_redis
    async def test_cancel_transaction(self):
        listener = MockTaskListener(self._app)
        listener.start_task_listener()
        tx = create_transaction(nonce=0, gasprice=10 ** 10, startgas=21000, to=TEST_ADDRESS, value=10 ** 18)
        tx = sign_transaction(tx, FAUCET_PRIVATE_KEY)

        tx_hash = calculate_transaction_hash(tx)
        from_address = FAUCET_ADDRESS
        to_address = TEST_ADDRESS

        async with self.pool.acquire() as con:
            await con.execute("INSERT INTO transactions (hash, from_address, to_address, nonce, value, gas, gas_price, data, v, r, s, status) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
                              tx_hash, from_address, to_address, tx.nonce,
                              hex(tx.value), hex(tx.startgas), hex(tx.gasprice),
                              data_encoder(tx.data), hex(tx.v), hex(tx.r), hex(tx.s),
                              'queued')

        signature = personal_sign(FAUCET_PRIVATE_KEY, "Cancel transaction " + tx_hash)

        resp = await self.fetch("/tx/cancel", method="POST", body={"tx_hash": tx_hash, "signature": signature})
        self.assertResponseCodeEqual(resp, 204)

        tx_id, status = await listener.get()
        self.assertEqual(tx_id, 1)
        self.assertEqual(status, 'error')

        await listener.stop_task_listener()

    @gen_test(timeout=15)
    @requires_database
    @requires_redis
    async def test_cancel_transaction_fails_if_not_queued(self):
        tx = create_transaction(nonce=0, gasprice=10 ** 10, startgas=21000, to=TEST_ADDRESS, value=10 ** 18)
        tx = sign_transaction(tx, FAUCET_PRIVATE_KEY)

        tx_hash = calculate_transaction_hash(tx)
        from_address = FAUCET_ADDRESS
        to_address = TEST_ADDRESS

        async with self.pool.acquire() as con:
            await con.execute("INSERT INTO transactions (hash, from_address, to_address, nonce, value, gas, gas_price, data, v, r, s, status) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
                              tx_hash, from_address, to_address, tx.nonce,
                              hex(tx.value), hex(tx.startgas), hex(tx.gasprice),
                              data_encoder(tx.data), hex(tx.v), hex(tx.r), hex(tx.s),
                              'unconfirmed')

        signature = personal_sign(FAUCET_PRIVATE_KEY, "Cancel transaction " + tx_hash)

        resp = await self.fetch("/tx/cancel", method="POST", body={"tx_hash": tx_hash, "signature": signature})
        self.assertResponseCodeEqual(resp, 400)

        async with self.pool.acquire() as con:
            await con.execute("UPDATE transactions SET status = 'confirmed'")

        resp = await self.fetch("/tx/cancel", method="POST", body={"tx_hash": tx_hash, "signature": signature})
        self.assertResponseCodeEqual(resp, 400)

        async with self.pool.acquire() as con:
            await con.execute("UPDATE transactions SET status = 'error'")

        resp = await self.fetch("/tx/cancel", method="POST", body={"tx_hash": tx_hash, "signature": signature})
        self.assertResponseCodeEqual(resp, 400)

    @gen_test(timeout=15)
    @requires_database
    @requires_redis
    async def test_cannot_cancel_others_transactions(self):
        tx = create_transaction(nonce=0, gasprice=10 ** 10, startgas=21000, to=TEST_ADDRESS, value=10 ** 18)
        tx = sign_transaction(tx, FAUCET_PRIVATE_KEY)

        tx_hash = calculate_transaction_hash(tx)
        from_address = FAUCET_ADDRESS
        to_address = TEST_ADDRESS

        async with self.pool.acquire() as con:
            await con.execute("INSERT INTO transactions (hash, from_address, to_address, nonce, value, gas, gas_price, data, v, r, s, status) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
                              tx_hash, from_address, to_address, tx.nonce,
                              hex(tx.value), hex(tx.startgas), hex(tx.gasprice),
                              data_encoder(tx.data), hex(tx.v), hex(tx.r), hex(tx.s),
                              'queued')

        signature = personal_sign(TEST_PRIVATE_KEY, "Cancel transaction " + tx_hash)

        resp = await self.fetch("/tx/cancel", method="POST", body={"tx_hash": tx_hash, "signature": signature})
        self.assertResponseCodeEqual(resp, 400)
