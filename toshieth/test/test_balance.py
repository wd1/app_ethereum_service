from toshi.test.base import AsyncHandlerTest
from toshieth.app import urls
from tornado.escape import json_decode
from tornado.testing import gen_test
from toshi.test.database import requires_database
from toshi.test.ethereum.parity import requires_parity
from toshi.test.ethereum.faucet import FaucetMixin, FAUCET_ADDRESS
from toshi.utils import parse_int

from toshi.handlers import BaseHandler
from toshi.database import DatabaseMixin
from toshi.ethereum.mixin import EthereumMixin
from toshieth.mixins import BalanceMixin

from toshi.ethereum.tx import DEFAULT_STARTGAS, DEFAULT_GASPRICE

class BalanceTest(FaucetMixin, AsyncHandlerTest):

    def get_urls(self):
        return urls

    def get_url(self, path):
        path = "/v1{}".format(path)
        return super().get_url(path)

    @gen_test(timeout=30)
    @requires_database
    @requires_parity
    async def test_get_balance(self):

        addr = '0x39bf9e501e61440b4b268d7b2e9aa2458dd201bb'
        val = 761751855997712

        await self.faucet(addr, val)

        resp = await self.fetch('/balance/{}'.format(addr))

        self.assertEqual(resp.code, 200)

        data = json_decode(resp.body)
        self.assertEqual(parse_int(data['confirmed_balance']), val)
        self.assertEqual(parse_int(data['unconfirmed_balance']), val)

    @gen_test(timeout=30)
    @requires_database
    @requires_parity
    async def test_get_balance_of_empty_address(self):

        addr = '0x39bf9e501e61440b4b268d7b2e9aa2458dd201bb'

        resp = await self.fetch('/balance/{}'.format(addr))

        self.assertEqual(resp.code, 200)

        data = json_decode(resp.body)
        self.assertEqual(data['confirmed_balance'], "0x0")
        self.assertEqual(data['unconfirmed_balance'], "0x0")

    @gen_test(timeout=30)
    @requires_database
    @requires_parity
    async def test_get_balance_with_unconfirmed_txs(self):

        tx1_hash = '0x2f321aa116146a9bc62b61c76508295f708f42d56340c9e613ebfc27e33f240c'
        tx2_hash = '0x2f321aa116146a9bc62b61c76508295f708f42d56340c9e613ebfc27e33f240d'
        addr = '0x39bf9e501e61440b4b268d7b2e9aa2458dd201bb'
        val = 761751855997712

        await self.faucet(addr, val)

        async with self.pool.acquire() as con:
            await con.execute(
                "INSERT INTO transactions (hash, from_address, to_address, nonce, value, gas, gas_price) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7)",
                tx1_hash, FAUCET_ADDRESS, addr, 0, val, DEFAULT_STARTGAS, DEFAULT_GASPRICE)
            await con.execute(
                "INSERT INTO transactions (hash, from_address, to_address, nonce, value, gas, gas_price, status) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                tx2_hash, FAUCET_ADDRESS, addr, 1, val,
                DEFAULT_STARTGAS, DEFAULT_GASPRICE,
                'unconfirmed')

        resp = await self.fetch('/balance/{}'.format(addr))

        self.assertEqual(resp.code, 200)

        data = json_decode(resp.body)
        self.assertEqual(parse_int(data['confirmed_balance']), val)
        self.assertEqual(parse_int(data['unconfirmed_balance']), val * 3)

    @gen_test(timeout=30)
    @requires_database
    @requires_parity
    async def test_get_balance_with_error_txs(self):

        tx1_hash = '0x2f321aa116146a9bc62b61c76508295f708f42d56340c9e613ebfc27e33f240c'
        tx2_hash = '0x2f321aa116146a9bc62b61c76508295f708f42d56340c9e613ebfc27e33f240d'
        tx3_hash = '0x2f321aa116146a9bc62b61c76508295f708f42d56340c9e613ebfc27e33f240e'
        addr = '0x39bf9e501e61440b4b268d7b2e9aa2458dd201bb'
        val = 761751855997712

        await self.faucet(addr, val)

        async with self.pool.acquire() as con:
            await con.execute(
                "INSERT INTO transactions (hash, from_address, to_address, nonce, value, gas, gas_price) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7)",
                tx1_hash, FAUCET_ADDRESS, addr, 0, val,
                DEFAULT_STARTGAS, DEFAULT_GASPRICE)
            await con.execute(
                "INSERT INTO transactions (hash, from_address, to_address, nonce, value, gas, gas_price, status) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                tx2_hash, FAUCET_ADDRESS, addr, 1, val,
                DEFAULT_STARTGAS, DEFAULT_GASPRICE,
                'unconfirmed')
            await con.execute(
                "INSERT INTO transactions (hash, from_address, to_address, nonce, value, gas, gas_price, status) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                tx3_hash, FAUCET_ADDRESS, addr, 2, val,
                DEFAULT_STARTGAS, DEFAULT_GASPRICE,
                'error')

        resp = await self.fetch('/balance/{}'.format(addr))

        self.assertEqual(resp.code, 200)

        data = json_decode(resp.body)
        self.assertEqual(parse_int(data['confirmed_balance']), val)
        self.assertEqual(parse_int(data['unconfirmed_balance']), val * 3)

    @gen_test(timeout=30)
    @requires_database
    @requires_parity
    async def test_get_balance_with_queued_items(self):

        tx1_hash = '0x2f321aa116146a9bc62b61c76508295f708f42d56340c9e613ebfc27e33f240c'
        tx2_hash = '0x2f321aa116146a9bc62b61c76508295f708f42d56340c9e613ebfc27e33f240d'
        tx3_hash = '0x2f321aa116146a9bc62b61c76508295f708f42d56340c9e613ebfc27e33f240e'
        addr = '0x39bf9e501e61440b4b268d7b2e9aa2458dd201bb'
        addr2 = '0x66c3dcc38542467eb6ddeef194add1d9eaaf05e0'
        val = 76175185599771243
        sent_val = 2 * 10 ** 10

        await self.faucet(addr, val)

        async with self.pool.acquire() as con:
            await con.execute(
                "INSERT INTO transactions (hash, from_address, to_address, nonce, value, gas, gas_price) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7)",
                tx1_hash, addr, addr2, 0, sent_val,
                DEFAULT_STARTGAS, DEFAULT_GASPRICE)
            await con.execute(
                "INSERT INTO transactions (hash, from_address, to_address, nonce, value, gas, gas_price, status) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                tx2_hash, addr, addr2, 1, sent_val,
                DEFAULT_STARTGAS, DEFAULT_GASPRICE,
                'queued')
            await con.execute(
                "INSERT INTO transactions (hash, from_address, to_address, nonce, value, gas, gas_price, status) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                tx3_hash, addr, addr2, 2, sent_val,
                DEFAULT_STARTGAS, DEFAULT_GASPRICE,
                'unconfirmed')

        resp = await self.fetch('/balance/{}'.format(addr))

        self.assertEqual(resp.code, 200)
        data = json_decode(resp.body)
        self.assertEqual(parse_int(data['confirmed_balance']), val)
        self.assertEqual(parse_int(data['unconfirmed_balance']),
                         val - ((sent_val + (DEFAULT_STARTGAS * DEFAULT_GASPRICE)) * 3))

        resp = await self.fetch('/balance/{}'.format(addr2))

        self.assertEqual(resp.code, 200)
        data = json_decode(resp.body)
        self.assertEqual(parse_int(data['confirmed_balance']), 0)
        self.assertEqual(parse_int(data['unconfirmed_balance']), sent_val * 3)

class SimpleHandler(BalanceMixin, EthereumMixin, DatabaseMixin, BaseHandler):

    async def get(self, address):

        confirmed, unconfirmed, _, _ = await self.get_balances(address, include_queued=False)

        self.write({
            "confirmed_balance": hex(confirmed),
            "unconfirmed_balance": hex(unconfirmed)
        })

class BalanceTest2(FaucetMixin, AsyncHandlerTest):

    def get_urls(self):
        return [('/(.+)', SimpleHandler)]

    @gen_test(timeout=30)
    @requires_database
    @requires_parity
    async def test_get_balance_without_queued_items(self):

        tx0_hash = '0x2f321aa116146a9bc62b61c76508295f708f42d56340c9e613ebfc27e33f240b'
        tx1_hash = '0x2f321aa116146a9bc62b61c76508295f708f42d56340c9e613ebfc27e33f240c'
        tx2_hash = '0x2f321aa116146a9bc62b61c76508295f708f42d56340c9e613ebfc27e33f240d'
        tx3_hash = '0x2f321aa116146a9bc62b61c76508295f708f42d56340c9e613ebfc27e33f240e'
        addr = '0x39bf9e501e61440b4b268d7b2e9aa2458dd201bb'
        addr2 = '0x66c3dcc38542467eb6ddeef194add1d9eaaf05e0'
        val = 76175185599771243
        sent_val = 2 * 10 ** 10

        await self.faucet(addr, val)

        async with self.pool.acquire() as con:
            await con.execute(
                "INSERT INTO transactions (hash, from_address, to_address, nonce, value, gas, gas_price, status) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                tx0_hash, addr, addr2, 0, sent_val,
                DEFAULT_STARTGAS, DEFAULT_GASPRICE, 'confirmed')
            await con.execute(
                "INSERT INTO transactions (hash, from_address, to_address, nonce, value, gas, gas_price) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7)",
                tx1_hash, addr, addr2, 0, sent_val,
                DEFAULT_STARTGAS, DEFAULT_GASPRICE)
            await con.execute(
                "INSERT INTO transactions (hash, from_address, to_address, nonce, value, gas, gas_price, status) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                tx2_hash, addr, addr2, 1, sent_val,
                DEFAULT_STARTGAS, DEFAULT_GASPRICE,
                'queued')
            await con.execute(
                "INSERT INTO transactions (hash, from_address, to_address, nonce, value, gas, gas_price, status) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                tx3_hash, addr, addr2, 2, sent_val,
                DEFAULT_STARTGAS, DEFAULT_GASPRICE,
                'unconfirmed')

        resp = await self.fetch('/{}'.format(addr))

        self.assertEqual(resp.code, 200)
        data = json_decode(resp.body)
        self.assertEqual(parse_int(data['confirmed_balance']), val)
        self.assertEqual(parse_int(data['unconfirmed_balance']),
                         val - (sent_val + (DEFAULT_STARTGAS * DEFAULT_GASPRICE)))

        resp = await self.fetch('/{}'.format(addr2))

        self.assertEqual(resp.code, 200)
        data = json_decode(resp.body)
        self.assertEqual(parse_int(data['confirmed_balance']), 0)
        self.assertEqual(parse_int(data['unconfirmed_balance']), sent_val)
