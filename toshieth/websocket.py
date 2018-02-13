import asyncio
import os
import uuid
import time
import traceback

import tornado.ioloop
import tornado.websocket
import tornado.web

from datetime import datetime
from toshi.database import DatabaseMixin
from toshi.handlers import RequestVerificationMixin
from toshi.utils import validate_address, validate_hex_string
from toshi.tasks import TaskHandler
from toshi.sofa import SofaPayment
from toshi.utils import parse_int
from toshi.ethereum.utils import encode_topic, decode_event_data
from toshi.ethereum.mixin import EthereumMixin

from toshi.log import log
from toshi.jsonrpc.errors import JsonRPCInvalidParamsError
from .jsonrpc import ToshiEthJsonRPC

class WebsocketJsonRPCHandler(ToshiEthJsonRPC):

    """Special handling for subscribe/unsubscribe when handled over
    websockets
    """

    def __init__(self, user_toshi_id, application, request_handler):
        super().__init__(user_toshi_id, application, request_handler.request)
        self.request_handler = request_handler

    async def subscribe(self, *addresses):
        if not addresses:
            raise JsonRPCInvalidParamsError(data={'id': 'bad_arguments', 'message': 'Bad Arguments'})

        for address in addresses:
            if not validate_address(address):
                raise JsonRPCInvalidParamsError(data={'id': 'bad_arguments', 'message': 'Bad Arguments'})

        try:
            await self.request_handler.subscribe(addresses)
        except:
            raise

        return True

    async def unsubscribe(self, *addresses):
        for address in addresses:
            if not validate_address(address):
                raise JsonRPCInvalidParamsError(data={'id': 'bad_arguments', 'message': 'Bad Arguments'})

        await self.request_handler.unsubscribe(addresses)

        return True

    def list_subscriptions(self):

        return list(self.request_handler.subscription_ids)

    async def filter(self, *, address=None, topic=None):
        if address is not None:
            if not validate_address(address):
                raise JsonRPCInvalidParamsError(data={'id': 'bad_arguments', 'message': 'Invalid Adddress'})
        if topic is not None:
            try:
                topic_id, topic = encode_topic(topic)
            except ValueError:
                raise JsonRPCInvalidParamsError(data={'id': 'bad_arguments', 'message': 'Invalid Topic'})

        filter_id = await self.request_handler.filter(address, topic_id, topic)
        return filter_id

    async def remove_filters(self, *filter_ids):
        for filter_id in filter_ids:
            if not validate_hex_string("0x" + filter_id):
                raise JsonRPCInvalidParamsError(data={'id': 'bad_arguments', 'message': 'Bad Arguments'})
        await self.request_handler.remove_filters(filter_ids)
        return True

    def get_timestamp(self):
        return int(time.time())

    async def list_payment_updates(self, address, start_time, end_time=None):

        try:
            return (await self._list_payment_updates(address, start_time, end_time))
        except:
            raise

    async def _list_payment_updates(self, address, start_time, end_time=None):

        if end_time is None:
            end_time = datetime.utcnow()
        elif not isinstance(end_time, datetime):
            end_time = datetime.utcfromtimestamp(end_time)
        if not isinstance(start_time, datetime):
            start_time = datetime.utcfromtimestamp(start_time)

        async with self.db:
            txs = await self.db.fetch(
                "SELECT * FROM transactions WHERE "
                "(from_address = $1 OR to_address = $1) AND "
                "updated > $2 AND updated < $3"
                "ORDER BY transaction_id ASC",
                address, start_time, end_time)
        payments = []
        for tx in txs:
            status = tx['status']
            if status is None or status == 'queued':
                status = 'unconfirmed'
            value = parse_int(tx['value'])
            if value is None:
                value = 0
            else:
                value = hex(value)
            # if the tx was created before the start time, send the unconfirmed
            # message as well.
            if status == 'confirmed' and tx['created'] > start_time:
                payments.append(SofaPayment(
                    status='unconfirmed', txHash=tx['hash'],
                    value=value, fromAddress=tx['from_address'],
                    toAddress=tx['to_address'],
                    networkId=self.application.config['ethereum']['network_id']
                ).render())
            payments.append(SofaPayment(
                status=status, txHash=tx['hash'],
                value=value, fromAddress=tx['from_address'],
                toAddress=tx['to_address'],
                networkId=self.application.config['ethereum']['network_id']
            ).render())

        return payments

class WebsocketHandler(tornado.websocket.WebSocketHandler, DatabaseMixin, EthereumMixin, RequestVerificationMixin):

    KEEP_ALIVE_TIMEOUT = 30

    @tornado.web.asynchronous
    def get(self, *args, **kwargs):

        if self.is_request_signed():
            self.user_toshi_id = self.verify_request()
        else:
            # assign a fake toshi_id
            self.user_toshi_id = "0x00000000000000000000{}".format(os.urandom(10).hex())
        self.subscription_ids = set()
        self.filter_ids = set()
        return super().get(*args, **kwargs)

    def open(self):

        self.session_id = uuid.uuid4().hex
        self.io_loop = tornado.ioloop.IOLoop.current()
        self.schedule_ping()

    def on_close(self):
        if hasattr(self, '_pingcb'):
            self.io_loop.remove_timeout(self._pingcb)
        self.io_loop.add_callback(self.unsubscribe, self.subscription_ids)
        self.io_loop.add_callback(self.remove_filters, list(self.filter_ids))

    def schedule_ping(self):
        self._pingcb = self.io_loop.call_later(self.KEEP_ALIVE_TIMEOUT, self.send_ping)

    def send_ping(self):
        try:
            self.ping(os.urandom(1))
        except tornado.websocket.WebSocketClosedError:
            pass

    def on_pong(self, data):
        self.schedule_ping()

    async def _on_message(self, message):
        try:
            response = await WebsocketJsonRPCHandler(
                self.user_toshi_id, self.application, self)(message)
            if response:
                self.write_message(response)
        except:
            log.exception("unexpected error handling message: {}".format(message))
            raise

    def on_message(self, message):
        if message is None:
            return
        tornado.ioloop.IOLoop.current().add_callback(self._on_message, message)

    async def subscribe(self, addresses):
        async with self.db.acquire() as db:
            for address in addresses:
                await db.execute(
                    "INSERT INTO notification_registrations (toshi_id, service, registration_id, eth_address) "
                    "VALUES ($1, $2, $3, $4) ON CONFLICT (toshi_id, service, registration_id, eth_address) DO NOTHING",
                    self.user_toshi_id, 'ws', self.session_id, address)
            await db.commit()

        for address in addresses:
            self.application.task_listener.subscribe(
                address, self.send_transaction_notification)
        self.subscription_ids.update(addresses)

    async def unsubscribe(self, addresses):
        self.subscription_ids.difference_update(addresses)
        async with self.db.acquire() as db:
            for address in addresses:
                await db.execute(
                    "DELETE FROM notification_registrations WHERE toshi_id = $1 AND service = $2 AND registration_id = $3 AND eth_address = $4",
                    self.user_toshi_id, 'ws', self.session_id, address)
            await db.commit()
        for address in addresses:
            self.application.task_listener.unsubscribe(
                address, self.send_transaction_notification)

    def send_transaction_notification(self, subscription_id, message):
        # make sure things are still connected
        if self.ws_connection is None:
            return

        self.write_message({
            "jsonrpc": "2.0",
            "method": "subscription",
            "params": {
                "subscription": subscription_id,
                "message": message
            }
        })

    async def filter(self, contract_address, topic_id, topic):
        new_filter_id = uuid.uuid4().hex
        async with self.db.acquire() as db:
            filter_id = await db.fetchval(
                "INSERT INTO filter_registrations (filter_id, registration_id, contract_address, topic_id, topic) "
                "VALUES ($1, $2, $3, $4, $5) ON CONFLICT (registration_id, contract_address, topic_id) "
                "DO UPDATE SET registration_id = EXCLUDED.registration_id "
                "RETURNING filter_id",
                new_filter_id, self.session_id, contract_address, topic_id, topic)
            if new_filter_id == filter_id:
                await db.commit()
                self.filter_ids.add(filter_id)
        self.application.task_listener.filter(
            filter_id, self.send_filter_notification)
        return filter_id

    async def remove_filters(self, filter_ids):
        if not isinstance(filter_ids, list):
            filter_ids = [filter_ids]
        async with self.db.acquire() as db:
            await db.execute(
                "DELETE FROM filter_registrations WHERE filter_id = ANY($1) AND registration_id = $2",
                filter_ids, self.session_id)
            await db.commit()

        for filter_id in filter_ids:
            self.application.task_listener.remove_filter(
                filter_id, self.send_filter_notification)

    def send_filter_notification(self, filter_id, topic, data):
        # make sure things are still connected
        if self.ws_connection is None:
            return

        args = decode_event_data(topic, data)

        self.write_message({
            "jsonrpc": "2.0",
            "method": "filter",
            "params": {
                "filter_id": filter_id,
                "topic": topic,
                "arguments": args
            }
        })

class WebsocketNotificationHandler(TaskHandler):

    async def send_notification(self, subscription_id, message):
        if subscription_id in self.application.callbacks:
            for callback in self.application.callbacks[subscription_id]:
                try:
                    f = callback(subscription_id, message)
                    if asyncio.iscoroutine(f):
                        await f
                except:
                    traceback.print_exc()

    async def send_filter_notification(self, filter_id, topic, data):
        if filter_id in self.application.filter_callbacks:
            for callback in self.application.filter_callbacks[filter_id]:
                try:
                    f = callback(filter_id, topic, data)
                    if asyncio.iscoroutine(f):
                        await f
                except:
                    traceback.print_exc()
