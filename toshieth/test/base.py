import asyncio

import toshieth.monitor
import toshieth.manager
import toshieth.push_service

from toshi.test.base import AsyncHandlerTest
from toshieth.app import Application, urls
from tornado.escape import json_decode

from toshi.ethereum.utils import private_key_to_address
from toshi.ethereum.tx import sign_transaction
from toshi.ethereum.utils import prepare_ethereum_jsonrpc_client

from toshi.test.database import requires_database
from toshi.test.redis import requires_redis
from toshi.test.ethereum.parity import requires_parity

class EthServiceBaseTest(AsyncHandlerTest):

    APPLICATION_CLASS = Application

    def get_urls(self):
        return urls

    def get_url(self, path):
        path = "/v1{}".format(path)
        return super().get_url(path)

    def setUp(self):
        # add fake redis config to make sure task_listener is created
        super().setUp(extraconf={'redis': {'unix_socket_path': '/dev/null', 'db': '0'}})

    async def wait_on_tx_confirmation(self, tx_hash, interval_check_callback=None, check_db=False):
        while True:
            resp = await self.fetch("/tx/{}".format(tx_hash))
            self.assertEqual(resp.code, 200)
            body = json_decode(resp.body)
            if body is None or body['blockNumber'] is None:
                if interval_check_callback:
                    f = interval_check_callback()
                    if asyncio.iscoroutine(f):
                        await f
                await asyncio.sleep(0.1)
            else:
                if check_db:
                    while True:
                        async with self.pool.acquire() as con:
                            row = await con.fetchrow("SELECT * FROM transactions WHERE hash = $1 AND status = 'confirmed'", tx_hash)
                        if row:
                            break
                        await asyncio.sleep(0.01)
                # make sure the last_blocknumber has been saved to the db before returning
                while True:
                    async with self.pool.acquire() as con:
                        row = await con.fetchrow("SELECT blocknumber FROM last_blocknumber")
                    if row and row['blocknumber'] >= int(body['blockNumber'], 16):
                        return body
                    await asyncio.sleep(0.01)

    async def get_tx_skel(self, from_key, to_addr, val, nonce=None, gas_price=None, gas=None, data=None):
        from_addr = private_key_to_address(from_key)
        body = {
            "from": from_addr,
            "to": to_addr,
            "value": val
        }
        if nonce is not None:
            body['nonce'] = nonce
        if gas_price is not None:
            body['gasPrice'] = gas_price
        if gas is not None:
            body['gas'] = gas
        if data is not None:
            body['data'] = data

        resp = await self.fetch("/tx/skel", method="POST", body=body)

        self.assertResponseCodeEqual(resp, 200, resp.body)

        body = json_decode(resp.body)

        tx = body['tx']

        return tx

    async def sign_and_send_tx(self, from_key, tx, expected_response_code=200):

        tx = sign_transaction(tx, from_key)

        body = {
            "tx": tx
        }

        resp = await self.fetch("/tx", method="POST", body=body)

        self.assertResponseCodeEqual(resp, expected_response_code, resp.body)
        if expected_response_code == 200:
            body = json_decode(resp.body)
            tx_hash = body['tx_hash']
            return tx_hash
        return None

    async def send_tx(self, from_key, to_addr, val, nonce=None, data=None, gas=None, gas_price=None):

        tx = await self.get_tx_skel(from_key, to_addr, val, nonce=nonce, data=data, gas=gas, gas_price=gas_price)
        return await self.sign_and_send_tx(from_key, tx)

    @property
    def network_id(self):
        return int(self._app.config['ethereum']['network_id'])

    @property
    def eth(self):
        if 'ethereum' not in self._app.config:
            raise Exception("Missing ethereum configuration")
        if not hasattr(self, '_eth_jsonrpc_client'):
            self._eth_jsonrpc_client = prepare_ethereum_jsonrpc_client(self._app.config['ethereum'])
        return self._eth_jsonrpc_client

def requires_block_monitor(func=None, cls=toshieth.monitor.BlockMonitor, pass_monitor=False, begin_started=True):
    """Used to ensure all database connections are returned to the pool
    before finishing the test"""

    def wrap(fn):

        async def wrapper(self, *args, **kwargs):

            if 'ethereum' not in self._app.config:
                raise Exception("Missing ethereum config from setup")

            monitor = cls(config=self._app.config,
                          connection_pool=self._app.connection_pool,
                          redis_connection_pool=self._app.redis_connection_pool)

            if begin_started:
                await monitor.start()

            if pass_monitor:
                if pass_monitor is True:
                    kwargs['monitor'] = monitor
                else:
                    kwargs[pass_monitor] = monitor

            try:
                f = fn(self, *args, **kwargs)
                if asyncio.iscoroutine(f):
                    await f
            finally:
                await monitor.shutdown(soft=True)

        return wrapper

    if func is not None:
        return wrap(func)
    else:
        return wrap

# overrides the start method to not trigger things that should only run when live
class TestTaskManager(toshieth.manager.TaskManager):
    def start(self):
        return self.task_listener.start_task_listener()

def requires_task_manager(func=None, pass_manager=False):
    """Used to ensure all database connections are returned to the pool
    before finishing the test"""

    def wrap(fn):

        async def wrapper(self, *args, **kwargs):

            if 'redis' not in self._app.config:
                raise Exception("Missing redis config from setup")

            task_manager = TestTaskManager(
                config=self._app.config,
                connection_pool=self._app.connection_pool,
                redis_connection_pool=self._app.redis_connection_pool)

            await task_manager.start()

            if pass_manager:
                if pass_manager is True:
                    kwargs['manager'] = task_manager
                else:
                    kwargs[pass_manager] = task_manager

            try:
                f = fn(self, *args, **kwargs)
                if asyncio.iscoroutine(f):
                    await f
            finally:
                await task_manager.shutdown(soft=True)

        return wrapper

    if func is not None:
        return wrap(func)
    else:
        return wrap

def requires_push_service(func, cls, pass_push_service=False, pass_push_client=False):
    """Used to ensure all database connections are returned to the pool
    before finishing the test"""

    def wrap(fn):

        async def wrapper(self, *args, **kwargs):

            if 'redis' not in self._app.config:
                raise Exception("Missing redis config from setup")

            pushclient = cls()
            push_service = toshieth.push_service.PushNotificationService(
                config=self._app.config,
                connection_pool=self._app.connection_pool,
                redis_connection_pool=self._app.redis_connection_pool,
                pushclient=pushclient)

            await push_service.start()

            if pass_push_service:
                if pass_push_service is True:
                    kwargs['push_service'] = push_service
                else:
                    kwargs[pass_push_service] = push_service
            if pass_push_client:
                if pass_push_client is True:
                    kwargs['push_client'] = pushclient
                else:
                    kwargs[pass_push_client] = pushclient

            try:
                f = fn(self, *args, **kwargs)
                if asyncio.iscoroutine(f):
                    await f
            finally:
                await push_service.shutdown(soft=False)

        return wrapper

    if func is not None:
        return wrap(func)
    else:
        return wrap

class MockPushClient:

    def __init__(self):
        self.send_queue = asyncio.Queue()

    async def send(self, toshi_id, network, device_toshi, data):
        if len(data) > 1 or 'message' not in data:
            raise NotImplementedError("Only data key allowed is 'message'")

        self.send_queue.put_nowait((device_toshi, data))

    def get(self):
        return self.send_queue.get()

def composed(*decs):
    """Decorator to combine multiple decorators together"""
    def deco(f):
        for dec in reversed(decs):
            args = ()
            kwargs = {}
            if not callable(dec):
                dec, *xargs = dec
                if len(xargs):
                    if isinstance(xargs[0], (list, tuple)):
                        args = xargs[0]
                        xargs = xargs[1:]
                    if xargs and isinstance(xargs[0], dict):
                        kwargs = xargs[0]
                        xargs = xargs[1:]
                    if xargs:
                        raise Exception("invalid arguments")
            f = dec(f, *args, **kwargs)
        return f
    return deco

def requires_full_stack(func=None, *, redis=None, parity=None, ethminer=None, manager=None, block_monitor=None, push_client=None):
    dec = composed(
        requires_database,
        (requires_redis, {'pass_redis': redis}),
        (requires_parity, {'pass_parity': parity, 'pass_ethminer': ethminer}),
        (requires_task_manager, {'pass_manager': manager}),
        (requires_block_monitor, {'pass_monitor': block_monitor}),
        (requires_push_service, (MockPushClient,), {'pass_push_client': push_client})
    )
    if func is None:
        return dec
    else:
        return dec(func)
