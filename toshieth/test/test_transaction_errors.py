import unittest

from tornado.testing import gen_test
from toshi.test.ethereum.faucet import FaucetMixin
from toshieth.test.base import requires_full_stack, EthServiceBaseTest
from toshi.ethereum.tx import sign_transaction, decode_transaction
from toshi.jsonrpc.client import JsonRPCClient

from toshieth.test.test_transaction import (
    TEST_PRIVATE_KEY as TEST_ID_KEY,
    TEST_ADDRESS as TEST_ID_ADDRESS,
    TEST_PRIVATE_KEY_2 as TEST_WALLET_KEY,
    TEST_ADDRESS_2 as TEST_WALLET_ADDRESS
)
from toshi.test.ethereum.parity import FAUCET_PRIVATE_KEY
from toshieth.test.test_pn_registration import TEST_GCM_ID, TEST_GCM_ID_2

class TestTransactionOverwrites(FaucetMixin, EthServiceBaseTest):

    @unittest.skip("TODO: figure out if it's still important to test this with parity >= 1.7.0")
    @gen_test(timeout=60)
    @requires_full_stack(ethminer=True, parity=True, block_monitor='monitor', push_client=True)
    async def test_transaction_overwrite_spam(self, *, ethminer, parity, monitor, push_client):

        no_to_spam = 10

        # make sure no blocks are confirmed
        ethminer.pause()

        # set up pn registrations
        async with self.pool.acquire() as con:
            await con.fetch("INSERT INTO notification_registrations (service, registration_id, toshi_id, eth_address) VALUES ($1, $2, $3, $4)",
                            'gcm', TEST_GCM_ID, TEST_ID_ADDRESS, TEST_WALLET_ADDRESS)

        # send initial tx
        tx1 = await self.get_tx_skel(FAUCET_PRIVATE_KEY, TEST_WALLET_ADDRESS, 10 ** 18)

        txs = []
        for i in range(no_to_spam):
            tx = await self.get_tx_skel(FAUCET_PRIVATE_KEY, TEST_WALLET_ADDRESS, i)
            txs.append(sign_transaction(tx, FAUCET_PRIVATE_KEY))

        tx1_hash = await self.sign_and_send_tx(FAUCET_PRIVATE_KEY, tx1)
        # wait for tx PN
        await push_client.get()

        # spam send txs manually
        rpcclient = JsonRPCClient(parity.dsn()['url'])
        for ntx in txs:
            await rpcclient.eth_sendRawTransaction(ntx)
            # force the pending transaction filter polling to
            # run after each new transaction is posted
            await monitor.filter_poll()
            # we expect two pns for each overwrite
            await push_client.get()
            await push_client.get()

        async with self.pool.acquire() as con:
            tx1_row = await con.fetchrow("SELECT * FROM transactions WHERE hash = $1", tx1_hash)
            tx_rows = await con.fetchrow("SELECT COUNT(*) FROM transactions")
            tx_rows_error = await con.fetchrow("SELECT COUNT(*) FROM transactions WHERE status = 'error'")

        self.assertEqual(tx1_row['status'], 'error')

        self.assertEqual(tx_rows['count'], no_to_spam + 1)
        self.assertEqual(tx_rows_error['count'], no_to_spam)

    @unittest.skip("TODO: figure out if it's still important to test this with parity >= 1.7.0")
    @gen_test(timeout=30)
    @requires_full_stack(ethminer=True, parity=True, block_monitor='monitor', push_client=True)
    async def test_resend_old_after_overwrite(self, *, ethminer, parity, monitor, push_client):
        # make sure no blocks are confirmed for the meantime
        ethminer.pause()

        # set up pn registrations
        async with self.pool.acquire() as con:
            await con.fetch("INSERT INTO notification_registrations (service, registration_id, toshi_id, eth_address) VALUES ($1, $2, $3, $4)",
                            'gcm', TEST_GCM_ID, TEST_ID_ADDRESS, TEST_WALLET_ADDRESS)

        # get tx skeleton
        tx1 = await self.get_tx_skel(FAUCET_PRIVATE_KEY, TEST_WALLET_ADDRESS, 10 ** 18)
        tx2 = await self.get_tx_skel(FAUCET_PRIVATE_KEY, TEST_WALLET_ADDRESS, 0)
        self.assertEqual(decode_transaction(tx1).nonce, decode_transaction(tx2).nonce)
        # sign and send
        tx1_hash = await self.sign_and_send_tx(FAUCET_PRIVATE_KEY, tx1)
        # wait for tx PN
        await push_client.get()

        # send tx2 manually
        rpcclient = JsonRPCClient(parity.dsn()['url'])
        tx2_hash = await rpcclient.eth_sendRawTransaction(sign_transaction(tx2, FAUCET_PRIVATE_KEY))
        await monitor.filter_poll()
        _, pn = await push_client.get()
        _, pn = await push_client.get()

        # resend tx1 manually
        tx1_hash = await rpcclient.eth_sendRawTransaction(sign_transaction(tx1, FAUCET_PRIVATE_KEY))
        await monitor.filter_poll()
        _, pn = await push_client.get()
        _, pn = await push_client.get()

        async with self.pool.acquire() as con:
            tx1_row = await con.fetchrow("SELECT * FROM transactions WHERE hash = $1", tx1_hash)
            tx2_row = await con.fetchrow("SELECT * FROM transactions WHERE hash = $1", tx2_hash)

        self.assertEqual(tx1_row['status'], 'unconfirmed')
        self.assertEqual(tx2_row['status'], 'error')
