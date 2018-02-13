from tornado.testing import gen_test
from toshieth.test.base import EthServiceBaseTest, requires_full_stack
from toshi.test.ethereum.parity import FAUCET_PRIVATE_KEY
from toshi.ethereum.utils import data_encoder
from toshi.ethereum.tx import create_transaction, sign_transaction, encode_transaction


class NetworkIdTest(EthServiceBaseTest):

    @gen_test(timeout=30)
    @requires_full_stack
    async def test_unable_to_send_txs_for_other_networks(self):

        network_id = 1 # Test network id is 66

        tx = create_transaction(nonce=9, gasprice=20 * 10**9, startgas=21000,
                                to="0x3535353535353535353535353535353535353535",
                                value=10**18, data=b'', network_id=network_id)
        sign_transaction(tx, FAUCET_PRIVATE_KEY)

        resp = await self.fetch("/tx", method="POST", body={
            "tx": encode_transaction(tx)
        })

        self.assertEqual(resp.code, 400, resp.body)

    @gen_test(timeout=30)
    @requires_full_stack
    async def test_send_txs_for_all_networks(self):

        tx = create_transaction(nonce=1048576, gasprice=20 * 10**9, startgas=21000,
                                to="0x3535353535353535353535353535353535353535",
                                value=10**18, data=b'')
        sign_transaction(tx, FAUCET_PRIVATE_KEY)

        resp = await self.fetch("/tx", method="POST", body={
            "tx": encode_transaction(tx)
        })

        self.assertEqual(resp.code, 200, resp.body)

        await self.wait_on_tx_confirmation(data_encoder(tx.hash))
