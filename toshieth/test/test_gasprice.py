from tornado.escape import json_decode
from tornado.testing import gen_test
from toshieth.test.base import EthServiceBaseTest
from toshi.test.redis import requires_redis
from toshi.ethereum.tx import DEFAULT_GASPRICE

class GasPriceTest(EthServiceBaseTest):

    @gen_test(timeout=15)
    @requires_redis
    async def test_gas_station_gas_price(self):

        gas_price = 50000000000
        assert(gas_price != DEFAULT_GASPRICE)
        self.redis.set("gas_station_standard_gas_price", hex(gas_price))

        resp = await self.fetch("/gasprice")
        self.assertResponseCodeEqual(resp, 200)
        body = json_decode(resp.body)
        self.assertEqual(body['gas_price'], hex(gas_price))
