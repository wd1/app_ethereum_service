import asyncio
import os

from tornado.testing import gen_test
from tornado.escape import json_decode

from toshi.test.base import ToshiWebSocketJsonRPCClient
from toshieth.test.base import EthServiceBaseTest, requires_full_stack
from toshi.test.ethereum.faucet import FAUCET_PRIVATE_KEY, FAUCET_ADDRESS
from toshi.ethereum.tx import sign_transaction, data_encoder, data_decoder
from toshi.jsonrpc.client import JsonRPCClient
from ethereum.utils import sha3

from toshi.ethereum.contract import Contract

from toshieth.test.test_transaction import (
    TEST_PRIVATE_KEY as TEST_ID_KEY,
    TEST_ADDRESS as TEST_ID_ADDRESS,
    TEST_PRIVATE_KEY_2 as TEST_WALLET_KEY,
    TEST_ADDRESS_2 as TEST_WALLET_ADDRESS
)

class ContractTest(EthServiceBaseTest):

    @gen_test(timeout=60)
    @requires_full_stack(parity=True)
    async def test_raw_deploy_contract(self, *, parity):
        """Tests that sending a raw transaction with a contract deployment works"""

        # contract data
        data = "0x6060604052341561000c57fe5b6040516102b83803806102b8833981016040528080518201919050505b806000908051906020019061003f929190610047565b505b506100ec565b828054600181600116156101000203166002900490600052602060002090601f016020900481019282601f1061008857805160ff19168380011785556100b6565b828001600101855582156100b6579182015b828111156100b557825182559160200191906001019061009a565b5b5090506100c391906100c7565b5090565b6100e991905b808211156100e55760008160009055506001016100cd565b5090565b90565b6101bd806100fb6000396000f30060606040526000357c0100000000000000000000000000000000000000000000000000000000900463ffffffff168063cfae32171461003b575bfe5b341561004357fe5b61004b6100d4565b604051808060200182810382528381815181526020019150805190602001908083836000831461009a575b80518252602083111561009a57602082019150602081019050602083039250610076565b505050905090810190601f1680156100c65780820380516001836020036101000a031916815260200191505b509250505060405180910390f35b6100dc61017d565b60008054600181600116156101000203166002900480601f0160208091040260200160405190810160405280929190818152602001828054600181600116156101000203166002900480156101725780601f1061014757610100808354040283529160200191610172565b820191906000526020600020905b81548152906001019060200180831161015557829003601f168201915b505050505090505b90565b6020604051908101604052806000815250905600a165627a7a72305820493059270656b40625319934bd6e91b0e68cf32c54c099dfc6cf540e40c91b9500290000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000000c68656c6c6f20776f726c64210000000000000000000000000000000000000000"

        resp = await self.fetch("/tx/skel", method="POST", body={
            "from": FAUCET_ADDRESS,
            "data": data
        })
        self.assertEqual(resp.code, 200)
        body = json_decode(resp.body)
        tx = sign_transaction(body['tx'], FAUCET_PRIVATE_KEY)
        resp = await self.fetch("/tx", method="POST", body={
            "tx": tx
        })
        self.assertEqual(resp.code, 200, resp.body)

        await self.wait_on_tx_confirmation(json_decode(resp.body)['tx_hash'])

        # test that contract txs from outside are handled correctly
        resp = await self.fetch_signed("/apn/register", signing_key=FAUCET_PRIVATE_KEY, method="POST", body={
            "registration_id": "blahblahblah"
        })

        resp = await self.fetch("/tx/skel", method="POST", body={
            "from": FAUCET_ADDRESS,
            "data": data
        })
        self.assertEqual(resp.code, 200)
        body = json_decode(resp.body)
        tx = sign_transaction(body['tx'], FAUCET_PRIVATE_KEY)

        # deploy manually
        rpcclient = JsonRPCClient(parity.dsn()['url'])
        tx_hash = await rpcclient.eth_sendRawTransaction(tx)

        await self.wait_on_tx_confirmation(tx_hash)
        await asyncio.sleep(5) # make sure the monitor has had a chance to process this

        async with self.pool.acquire() as con:
            rows = await con.fetch("SELECT * FROM transactions WHERE hash = $1", tx_hash)
        self.assertEqual(len(rows), 1)

    @gen_test(timeout=60)
    @requires_full_stack(parity=True)
    async def test_monitor_contract_events(self, *, parity):

        os.environ['ETHEREUM_NODE_URL'] = parity.dsn()['url']

        sourcecode1 = b"contract Test{event TestEvent(address sender,uint256 amount); event Something(uint256 one, bool two); function Test(){} function () payable {TestEvent(msg.sender,msg.value);}}"
        sourcecode2 = b"contract Test{event Something(uint256 one, bool two); function Test(){} function () payable {Something(1,true);}}"
        sourcecode3 = b"contract Test{event ArrayStuff(uint256[] arr, bytes8); function Test(){} function () payable {uint[] memory a = new uint[](2); a[0] = 0; a[1] = 1; ArrayStuff(a,'abcdefgh');}}"
        sourcecode4 = b"contract Test{event MatrixStuff(uint8[2][2][2] mtx); function Test(){} function () payable {MatrixStuff([[[0,1],[2,3]],[[4,5],[6,7]]]);}}"
        # TODO: structs, enums
        contract_name = "Test"
        constructor_data = []

        contract1 = await Contract.from_source_code(sourcecode1, contract_name, constructor_data=constructor_data, deployer_private_key=FAUCET_PRIVATE_KEY)
        contract2 = await Contract.from_source_code(sourcecode2, contract_name, constructor_data=constructor_data, deployer_private_key=FAUCET_PRIVATE_KEY)
        contract3 = await Contract.from_source_code(sourcecode3, contract_name, constructor_data=constructor_data, deployer_private_key=FAUCET_PRIVATE_KEY)
        contract4 = await Contract.from_source_code(sourcecode4, contract_name, constructor_data=constructor_data, deployer_private_key=FAUCET_PRIVATE_KEY)

        # since txs are from outside the system, need to wait until they're seen
        await asyncio.sleep(5)
        value = 10 ** 17

        con1 = ToshiWebSocketJsonRPCClient(self.get_url("/ws"), signing_key=FAUCET_PRIVATE_KEY)
        await con1.connect()
        await con1.call("filter", {"address": contract1.address, "topic": "TestEvent(address,uint)"})

        con2 = ToshiWebSocketJsonRPCClient(self.get_url("/ws"), signing_key=FAUCET_PRIVATE_KEY)
        await con2.connect()
        await con2.call("filter", {"address": contract2.address, "topic": "Something(uint,bool)"})

        con3 = ToshiWebSocketJsonRPCClient(self.get_url("/ws"), signing_key=FAUCET_PRIVATE_KEY)
        await con3.connect()
        await con3.call("filter", {"address": contract3.address, "topic": "ArrayStuff(uint[],bytes8)"})

        con4 = ToshiWebSocketJsonRPCClient(self.get_url("/ws"), signing_key=FAUCET_PRIVATE_KEY)
        await con4.connect()
        await con4.call("filter", {"address": contract4.address, "topic": "MatrixStuff(uint8[2][2][2])"})

        tx_hash1 = await self.send_tx(FAUCET_PRIVATE_KEY, contract1.address, value)
        tx_hash2 = await self.send_tx(FAUCET_PRIVATE_KEY, contract2.address, value)
        tx_hash3 = await self.send_tx(FAUCET_PRIVATE_KEY, contract3.address, value)
        tx_hash4 = await self.send_tx(FAUCET_PRIVATE_KEY, contract4.address, value)

        event1 = await con1.read()
        event2 = await con2.read()
        event3 = await con3.read()
        event4 = await con4.read()

        self.assertEqual(event1['params']['arguments'][0], FAUCET_ADDRESS)
        self.assertEqual(int(event1['params']['arguments'][1], 16), value)
        self.assertEqual(int(event2['params']['arguments'][0], 16), 1)
        self.assertEqual(event2['params']['arguments'][1], True)
        self.assertEqual(event3['params']['arguments'][0][0], "0x0")
        self.assertEqual(event3['params']['arguments'][0][1], "0x1")
        self.assertEqual(data_decoder(event3['params']['arguments'][1]), b'abcdefgh')
        self.assertEqual(event4['params']['arguments'][0][0][0][0], "0x0")
        self.assertEqual(event4['params']['arguments'][0][0][0][1], "0x1")
        self.assertEqual(event4['params']['arguments'][0][0][1][0], "0x2")
        self.assertEqual(event4['params']['arguments'][0][0][1][1], "0x3")
        self.assertEqual(event4['params']['arguments'][0][1][0][0], "0x4")
        self.assertEqual(event4['params']['arguments'][0][1][0][1], "0x5")
        self.assertEqual(event4['params']['arguments'][0][1][1][0], "0x6")
        self.assertEqual(event4['params']['arguments'][0][1][1][1], "0x7")

        contract_balance = json_decode((await self.fetch("/balance/{}".format(contract1.address))).body)
        self.assertEqual(int(contract_balance['confirmed_balance'][2:], 16), value)

        for tx_hash in [tx_hash1, tx_hash2, tx_hash3, tx_hash4]:
            trace = await self.eth.trace_replayTransaction(tx_hash)
            self.assertNotIn('error', trace)

    @gen_test(timeout=60)
    @requires_full_stack(parity=True)
    async def test_fail_when_contract_has_no_default_function(self, *, parity):

        os.environ['ETHEREUM_NODE_URL'] = parity.dsn()['url']

        value = 10 ** 17

        constructor_data = []
        sourcecode = b"contract Test{uint count; function Test(){count=0;} function add(){count+=1;}}"
        contract_name = "Test"
        constructor_data = []
        contract = await Contract.from_source_code(sourcecode, contract_name, constructor_data=constructor_data, deployer_private_key=FAUCET_PRIVATE_KEY)

        # wait for block processing to see the block that the contract was part of
        await asyncio.sleep(5)

        with self.assertRaises(AssertionError):
            await self.send_tx(FAUCET_PRIVATE_KEY, contract.address, value)

        with self.assertRaises(AssertionError):
            await self.send_tx(FAUCET_PRIVATE_KEY, contract.address, value, data="0x0123456789")

        tx_hash = await self.send_tx(FAUCET_PRIVATE_KEY, contract.address, value, gas=23000)

        await self.wait_on_tx_confirmation(tx_hash)

        trace = await self.eth.trace_replayTransaction(tx_hash)
        self.assertTrue(any('error' in t for t in trace['trace']))

        contract_balance = json_decode((await self.fetch("/balance/{}".format(contract.address))).body)
        self.assertEqual(int(contract_balance['confirmed_balance'][2:], 16), 0)
