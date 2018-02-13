import asyncio
from tornado.testing import gen_test

from toshieth.test.base import EthServiceBaseTest, requires_full_stack
from toshi.test.ethereum.parity import FAUCET_PRIVATE_KEY, ParityServer
from toshi.test.ethereum.ethminer import EthMiner
from toshi.ethereum.tx import decode_transaction
from toshi.ethereum.utils import data_decoder
from toshi.ethereum.tx import sign_transaction
from toshi.jsonrpc.client import JsonRPCClient

TEST_PRIVATE_KEY_1 = data_decoder("0xe8f32e723decf4051aefac8e2c93c9c5b214313817cdb01a1494b917c8436b35")
TEST_ADDRESS_1 = "0x056db290f8ba3250ca64a45d16284d04bc6f5fbf"

TEST_PRIVATE_KEY_2 = data_decoder("0x0ffdb88a7a0a40831ca0b19bd31f3f6085764ef8b7db1bd6b57072e5eaea24ff")
TEST_ADDRESS_2 = "0x35351b44e03ec8515664a955146bf9c6e503a381"

TEST_PRIVATE_KEY_3 = data_decoder("0xbc91668394b936efc05aecbf30213746ce7967374918ba75d0bc751a30463cd5")
TEST_ADDRESS_3 = "0x4aaddc8923dfbe6053291c8ba785af5bda49d905"

TEST_PRIVATE_KEY_4 = data_decoder("0x3d60b77d9bd1e63775e4666baeda12d1d236265a85a5a4b620bcbe47ca7bcde4")
TEST_ADDRESS_4 = "0xc4867418f9950f1c32f79fee9098f9058c06823c"

TEST_PRIVATE_KEY_5 = data_decoder("0x40ba61d699250a190ec87e78be117007c7acbacb2c18c88f649c31dfc28bfb2a")
TEST_ADDRESS_5 = "0x3fa28bbf036821db8342f66b7ce8ff583e0ebfd5"

TEST_PRIVATE_KEY_6 = data_decoder("0x006876936f5bf4f404a080e474f94aa3fcd7c413009533fcde51e951abb7f724")
TEST_ADDRESS_6 = "0xb33bcd070ed7c5effa861051a1ec25b818fea111"

TEST_ADDRESSES = [(TEST_ADDRESS_1, TEST_PRIVATE_KEY_1),
                  (TEST_ADDRESS_2, TEST_PRIVATE_KEY_2),
                  (TEST_ADDRESS_3, TEST_PRIVATE_KEY_3),
                  (TEST_ADDRESS_4, TEST_PRIVATE_KEY_4),
                  (TEST_ADDRESS_5, TEST_PRIVATE_KEY_5),
                  (TEST_ADDRESS_6, TEST_PRIVATE_KEY_6)]

def with_timeout(func=None, timeout=5):
    def wrap(fn):
        async def wrapper(self, *args, **kwargs):
            xtimeout = kwargs.pop('timeout', timeout)
            result = await asyncio.wait_for(fn(self, *args, **kwargs), timeout=xtimeout)
            return result
        return wrapper
    if func is not None:
        return wrap(func)
    else:
        return wrap

class TransactionOverwriteTest(EthServiceBaseTest):

    @gen_test(timeout=300)
    @requires_full_stack(ethminer=True, parity=True, push_client=True)
    async def test_tx_overwrite(self, *, ethminer, parity, push_client):
        """Tests that if a transaction with the same nonce and one the system knows about
        is sent from outside of the system and included in the block, that the error
        handling picks this up correctly"""

        # start 2nd parity server
        p2 = ParityServer(bootnodes=parity.dsn()['node'])
        e2 = EthMiner(jsonrpc_url=p2.dsn()['url'],
                      debug=False)
        rpcclient2 = JsonRPCClient(p2.dsn()['url'])

        addr1, pk1 = TEST_ADDRESSES[0]
        addr2, pk2 = TEST_ADDRESSES[1]
        addr3, pk3 = TEST_ADDRESSES[2]

        val = 1000 * 10 ** 18

        # send funds to address1
        f_tx_hash = await self.send_tx(FAUCET_PRIVATE_KEY, TEST_ADDRESS_1, val)

        # make sure sync is done
        while True:
            data2 = await rpcclient2.eth_getTransactionByHash(f_tx_hash)
            if data2 and data2['blockNumber'] is not None:
                break
            await asyncio.sleep(1)

        # make sure no blocks are mined
        ethminer.pause()
        e2.pause()

        # make sure transactions are "interesting" to the monitory
        async with self.pool.acquire() as con:
            for addr, pk in TEST_ADDRESSES[:3]:
                await con.fetch("INSERT INTO notification_registrations (service, registration_id, toshi_id, eth_address) VALUES ($1, $2, $3, $4)",
                                'gcm', "abc", addr, addr)

        # create two transactions with the same nonce and submit them both
        tx1 = await self.get_tx_skel(pk1, addr2, int(val / 3))
        tx2 = await self.get_tx_skel(pk1, addr3, int(val / 3), nonce=decode_transaction(tx1).nonce)
        tx2 = sign_transaction(tx2, pk1)
        tx_hash_2 = await rpcclient2.eth_sendRawTransaction(tx2)
        tx_hash_1 = await self.sign_and_send_tx(pk1, tx1)

        # start mining again
        e2.start()

        # wait for one of the two transactions to complete
        try:
            while True:
                async with self.pool.acquire() as con:
                    tx1_row = await con.fetchrow("SELECT * FROM transactions WHERE hash = $1", tx_hash_1)
                    tx2_row = await con.fetchrow("SELECT * FROM transactions WHERE hash = $1", tx_hash_2)
                if tx2_row is not None and tx2_row['status'] == 'confirmed':
                    # good!
                    break
                if tx1_row is not None and tx1_row['status'] == 'confirmed':
                    self.assertFail("tx1 confirmed, expected tx1 overwrite and tx2 confirmed")
                await asyncio.sleep(1)
        finally:
            e2.stop()
            p2.stop()

    @gen_test(timeout=30)
    @requires_full_stack(ethminer=True, parity=True, push_client=True)
    async def test_tx_overwrite_with_using_full_balance(self, *, ethminer, parity, push_client):

        balance = 10 ** 18
        value = 8 * 10 ** 17
        bal_hash = await self.send_tx(FAUCET_PRIVATE_KEY, TEST_ADDRESS_1, balance)

        while True:
            async with self.pool.acquire() as con:
                row = await con.fetchrow("SELECT * FROM transactions WHERE hash = $1", bal_hash)
            if row['status'] == 'confirmed':
                break
            await asyncio.sleep(0.1)

        ethminer.pause()

        tx_hash = await self.send_tx(TEST_PRIVATE_KEY_1, TEST_ADDRESS_2, value)
        async with self.pool.acquire() as con:
            tx = await con.fetchrow("SELECT * FROM transactions WHERE hash = $1", tx_hash)

        tx_hash_2 = await self.send_tx(TEST_PRIVATE_KEY_1, TEST_ADDRESS_2, value, nonce=tx['nonce'],
                                       gas_price=hex(int(tx['gas_price'], 16) + 10 ** 10))

        while True:
            async with self.pool.acquire() as con:
                tx = await con.fetchrow("SELECT * FROM transactions WHERE hash = $1", tx_hash)
                tx2 = await con.fetchrow("SELECT * FROM transactions WHERE hash = $1", tx_hash_2)
            if tx['status'] != 'error':
                continue
            if tx2['status'] == 'unconfirmed':
                break
            asyncio.sleep(0.1)
