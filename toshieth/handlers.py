# -*- coding: utf-8 -*-
from toshi.handlers import BaseHandler
from toshi.errors import JSONHTTPError
from toshi.jsonrpc.errors import JsonRPCInternalError
from toshi.database import DatabaseMixin
from toshi.ethereum.mixin import EthereumMixin
from toshi.jsonrpc.errors import JsonRPCError
from toshi.redis import RedisMixin
from toshi.analytics import AnalyticsMixin

from toshi.sofa import SofaPayment
from toshi.handlers import RequestVerificationMixin, SimpleFileHandler
from toshi.utils import validate_address, parse_int
from toshi.log import log, log_headers_on_error

from .mixins import BalanceMixin
from .jsonrpc import ToshiEthJsonRPC
from .utils import database_transaction_to_rlp_transaction
from toshi.ethereum.tx import transaction_to_json, DEFAULT_GASPRICE
from tornado.escape import json_encode

class TokenHandler(DatabaseMixin, SimpleFileHandler):

    async def get(self, symbol_png=None):

        if symbol_png:
            # remove .png suffix required by URL regex
            symbol = symbol_png[:-4]

            async with self.db:
                row = await self.db.fetchrow(
                    "SELECT * FROM tokens WHERE symbol = $1",
                    symbol
                )

            if row is None:
                raise HTTPError(404)

            await self.handle_file_response(
                data=row['icon'],
                content_type="image/png",
                etag=row['hash'],
                last_modified=row['last_modified']
            )

        else:
            # list available tokens
            async with self.db:
                rows = await self.db.fetch(
                    "SELECT symbol, name, decimals FROM tokens "
                    "ORDER BY symbol ASC"
                )

            tokens = [dict(symbol=r['symbol'],
                           name=r['name'],
                           decimals=r['decimals'],
                           icon_url="/token/{}.png".format(r['symbol']))
                      for r in rows]
            self.write({"tokens": tokens})


class BalanceHandler(DatabaseMixin, EthereumMixin, BaseHandler):

    async def get(self, address):

        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'GET')

        try:
            result = await ToshiEthJsonRPC(None, self.application, self.request).get_balance(address)
        except JsonRPCError as e:
            raise JSONHTTPError(400, body={'errors': [e.data]})

        self.write(result)

class TransactionSkeletonHandler(EthereumMixin, RedisMixin, BaseHandler):

    async def post(self):

        try:
            # normalize inputs
            if 'from' in self.json:
                self.json['from_address'] = self.json.pop('from')
            if 'to' in self.json:
                self.json['to_address'] = self.json.pop('to')
            elif 'to_address' not in self.json:
                self.json['to_address'] = None
            # the following are to deal with different representations
            # of the same concept from different places
            if 'gasPrice' in self.json:
                self.json['gas_price'] = self.json.pop('gasPrice')
            if 'gasprice' in self.json:
                self.json['gas_price'] = self.json.pop('gasprice')
            if 'startgas' in self.json:
                self.json['gas'] = self.json.pop('startgas')
            result = await ToshiEthJsonRPC(None, self.application, self.request).create_transaction_skeleton(**self.json)
        except JsonRPCError as e:
            log.warning("/tx/skel failed: " + json_encode(e.data) + "\" -> arguments: " + json_encode(self.json) + "\"")
            raise JSONHTTPError(400, body={'errors': [e.data]})
        except TypeError:
            log.warning("/tx/skel failed: bad arguments \"" + json_encode(self.json) + "\"")
            raise JSONHTTPError(400, body={'errors': [{'id': 'bad_arguments', 'message': 'Bad Arguments'}]})

        self.write(result)

class SendTransactionHandler(BalanceMixin, EthereumMixin, DatabaseMixin, RedisMixin, RequestVerificationMixin, BaseHandler):

    async def post(self):

        if self.is_request_signed():
            sender_toshi_id = self.verify_request()
        else:
            # this is an anonymous transaction
            sender_toshi_id = None

        try:
            result = await ToshiEthJsonRPC(sender_toshi_id, self.application, self.request).send_transaction(**self.json)
        except JsonRPCInternalError as e:
            raise JSONHTTPError(500, body={'errors': [e.data]})
        except JsonRPCError as e:
            raise JSONHTTPError(400, body={'errors': [e.data]})
        except TypeError:
            raise JSONHTTPError(400, body={'errors': [{'id': 'bad_arguments', 'message': 'Bad Arguments'}]})

        self.write({
            "tx_hash": result
        })

class TransactionHandler(EthereumMixin, DatabaseMixin, BaseHandler):

    async def get(self, tx_hash):

        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'GET')

        format = self.get_query_argument('format', 'rpc').lower()

        try:
            tx = await ToshiEthJsonRPC(None, self.application, self.request).get_transaction(tx_hash)
        except JsonRPCError as e:
            raise JSONHTTPError(400, body={'errors': [e.data]})

        if tx is None and format != 'sofa':
            raise JSONHTTPError(404, body={'error': [{'id': 'not_found', 'message': 'Not Found'}]})

        if format == 'sofa':

            async with self.db:
                row = await self.db.fetchrow(
                    "SELECT * FROM transactions where hash = $1 ORDER BY transaction_id DESC",
                    tx_hash)
            if row is None:
                raise JSONHTTPError(404, body={'error': [{'id': 'not_found', 'message': 'Not Found'}]})
            if tx is None:
                tx = transaction_to_json(database_transaction_to_rlp_transaction(row))
            if row['status'] == 'error':
                tx['error'] = True
            payment = SofaPayment.from_transaction(tx, networkId=self.application.config['ethereum']['network_id'])
            message = payment.render()
            self.set_header('Content-Type', 'text/plain')
            self.write(message.encode('utf-8'))

        else:

            self.write(tx)

class CancelTransactionHandler(EthereumMixin, DatabaseMixin, BaseHandler):

    async def post(self):

        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST')

        if 'tx_hash' not in self.json or 'signature' not in self.json:
            raise JSONHTTPError(400, body={'errors': [{'id': 'bad_arguments', 'message': 'Bad Arguments'}]})

        tx_hash = self.json['tx_hash']
        signature = self.json['signature']

        try:
            await ToshiEthJsonRPC(None, self.application, self.request).cancel_queued_transaction(tx_hash, signature)
        except JsonRPCError as e:
            raise JSONHTTPError(400, body={'errors': [e.data]})

        self.set_status(204)

class AddressHandler(DatabaseMixin, BaseHandler):

    async def get(self, address):

        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'GET')

        offset = parse_int(self.get_argument('offset', '0'))
        limit = parse_int(self.get_argument('limit', '10'))
        status = set([s.lower() for s in self.get_arguments('status')])
        direction = set([d.lower() for d in self.get_arguments('direction')])
        order = self.get_argument('order', 'desc').upper()

        if not validate_address(address) or \
           offset is None or \
           limit is None or \
           (status and not status.issubset(['confirmed', 'unconfirmed', 'queued', 'error'])) or \
           (direction and not direction.issubset(['in', 'out'])) or \
           (order not in ['DESC', 'ASC']):
            raise JSONHTTPError(400, body={'id': 'bad_arguments', 'message': 'Bad Arguments'})

        query = "SELECT * FROM transactions WHERE "
        args = [address, offset, limit]

        if len(direction) == 0 or len(direction) == 2:
            query += "(from_address = $1 OR to_address = $1) "
        elif 'in' in direction:
            query += "to_address = $1 "
        elif 'out' in direction:
            query += "from_address = $1 "

        if len(status) == 0:
            query += "AND (status != $4 OR status IS NULL) "
            args.append('error')
        else:
            status_query = []
            for s in status:
                if s == 'queued':
                    status_query.extend(["status = ${}".format(len(args) + 1), "status IS NULL"])
                else:
                    status_query.append("status = ${}".format(len(args) + 1))
                args.append(s)
            query += "AND (" + " OR ".join(status_query) + ") "

        query += "ORDER BY created {} OFFSET $2 LIMIT $3".format(order)

        async with self.db:
            rows = await self.db.fetch(query, *args)

        transactions = []
        for row in rows:
            transactions.append({
                "hash": row['hash'],
                "to": row['to_address'],
                "from": row['from_address'],
                "nonce": hex(row['nonce']),
                "value": row['value'],
                "gas": row['gas'],
                "gas_price": row['gas_price'],
                "created_data": row['created'].isoformat(),
                "confirmed_data": row['updated'].isoformat() if row['blocknumber'] else None,
                "status": row['status'] if row['status'] is not None else 'queued',
                "data": row['data']
            })
        resp = {
            "transactions": transactions,
            "offset": offset,
            "limit": limit,
            "order": order
        }
        if len(direction) == 1:
            resp['direction'] = direction.pop()
        if status:
            resp['status'] = "&".join(status)
        self.write(resp)

class GasPriceHandler(RedisMixin, BaseHandler):

    def get(self):

        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'GET')

        gas_station_gas_price = self.redis.get('gas_station_standard_gas_price')
        if gas_station_gas_price is None:
            gas_station_gas_price = hex(self.application.config['ethereum'].getint('default_gasprice', DEFAULT_GASPRICE))
        self.write({
            "gas_price": gas_station_gas_price
        })

class PNRegistrationHandler(RequestVerificationMixin, DatabaseMixin, BaseHandler):

    @log_headers_on_error
    async def post(self, service):
        toshi_id = self.verify_request()
        payload = self.json

        if not all(arg in payload for arg in ['registration_id']):
            raise JSONHTTPError(400, body={'errors': [{'id': 'bad_arguments', 'message': 'Bad Arguments'}]})

        # TODO: registration id verification

        # XXX: BACKWARDS COMPAT FOR OLD PN REGISTARTION
        # remove when no longer needed
        if 'address' not in payload:
            async with self.db:
                legacy = await self.db.fetch("SELECT eth_address FROM notification_registrations "
                                             "WHERE toshi_id = $1 AND service = 'LEGACY' AND registration_id = 'LEGACY'",
                                             toshi_id)
        else:
            legacy = False

        if legacy:

            async with self.db:

                for row in legacy:
                    eth_address = row['eth_address']
                    await self.db.execute(
                        "INSERT INTO notification_registrations (toshi_id, service, registration_id, eth_address) "
                        "VALUES ($1, $2, $3, $4) ON CONFLICT (toshi_id, service, registration_id, eth_address) DO NOTHING",
                        toshi_id, service, payload['registration_id'], eth_address)
                await self.db.execute(
                    "DELETE FROM notification_registrations "
                    "WHERE toshi_id = $1 AND service = 'LEGACY' AND registration_id = 'LEGACY'", toshi_id)
                await self.db.commit()

        else:

            # eth address verification (default to toshi_id if eth_address is not supplied)
            eth_address = payload['address'] if 'address' in payload else toshi_id
            if not validate_address(eth_address):
                raise JSONHTTPError(data={'id': 'bad_arguments', 'message': 'Bad Arguments'})

            async with self.db:

                await self.db.execute(
                    "INSERT INTO notification_registrations (toshi_id, service, registration_id, eth_address) "
                    "VALUES ($1, $2, $3, $4) ON CONFLICT (toshi_id, service, registration_id, eth_address) DO NOTHING",
                    toshi_id, service, payload['registration_id'], eth_address)

                # XXX: temporary fix for old ios versions sending their payment address as toshi_id
                # should be removed after enough time has passed that most people should be using the fixed version
                if eth_address != toshi_id:
                    # remove any apn registrations where toshi_id == eth_address for this eth_address
                    await self.db.execute(
                        "DELETE FROM notification_registrations "
                        "WHERE toshi_id = $1 AND eth_address = $1 AND service = 'apn'", eth_address)

                await self.db.commit()

        self.set_status(204)

class PNDeregistrationHandler(RequestVerificationMixin, AnalyticsMixin, DatabaseMixin, BaseHandler):

    async def post(self, service):

        toshi_id = self.verify_request()
        payload = self.json

        if 'registration_id' not in payload:
            raise JSONHTTPError(400, body={'errors': [{'id': 'bad_arguments', 'message': 'Bad Arguments'}]})

        # TODO: registration id verification

        # eth address verification (if none is supplied, delete all the matching addresses)
        eth_address = payload.get('address', None)
        if eth_address and not validate_address(eth_address):
            raise JSONHTTPError(data={'id': 'bad_arguments', 'message': 'Bad Arguments'})

        async with self.db:

            args = [toshi_id, service, payload['registration_id']]
            if eth_address:
                args.append(eth_address)
            await self.db.execute(
                "DELETE FROM notification_registrations WHERE toshi_id = $1 AND service = $2 AND registration_id = $3{}".format(
                    "AND eth_address = $4" if eth_address else ""),
                *args)

            await self.db.commit()

        self.set_status(204)
        self.track(toshi_id, "Deregistered ETH notifications")

class StatusHandler(RedisMixin, BaseHandler):

    def get(self):
        status = self.redis.get("monitor_sanity_check_ok")
        if status == "OK":
            self.write("OK")
        else:
            self.write("MONITOR SANITY CHECK FAILED")

class LegacyRegistrationHandler(RequestVerificationMixin, DatabaseMixin, BaseHandler):
    """backwards compatibility for old pn registration"""

    async def post(self):

        toshi_id = self.verify_request()
        payload = self.json

        if 'addresses' not in payload or len(payload['addresses']) == 0:
            raise JSONHTTPError(400, body={'errors': [{'id': 'bad_arguments', 'message': 'Bad Arguments'}]})

        addresses = payload['addresses']

        for address in addresses:
            if not validate_address(address):
                raise JSONHTTPError(400, body={'errors': [{'id': 'bad_arguments', 'message': 'Bad Arguments'}]})

        async with self.db:

            # see if this toshi_id is already registered, listening to it's own toshi_id
            rows = await self.db.fetch("SELECT * FROM notification_registrations "
                                       "WHERE toshi_id = $1 AND eth_address = $1 AND service != 'ws'",
                                       toshi_id)
            if rows:
                if len(rows) > 1:
                    log.warning("LEGACY REGISTRATION FOR '{}' HAS MORE THAN ONE DEVICE OR SERVICE".format(toshi_id))
                registration_id = rows[0]['registration_id']
                service = rows[0]['service']
            else:
                service = 'LEGACY'
                registration_id = 'LEGACY'

            # simply store all the entered addresses with no service/registrations id
            for address in addresses:
                await self.db.execute(
                    "INSERT INTO notification_registrations (toshi_id, service, registration_id, eth_address) "
                    "VALUES ($1, $2, $3, $4) ON CONFLICT (toshi_id, service, registration_id, eth_address) DO NOTHING",
                    toshi_id, service, registration_id, address)

            await self.db.commit()

        self.set_status(204)

class LegacyDeregistrationHandler(RequestVerificationMixin, AnalyticsMixin, DatabaseMixin, BaseHandler):

    async def post(self):

        toshi_id = self.verify_request()
        payload = self.json

        if 'addresses' not in payload or len(payload['addresses']) == 0:
            raise JSONHTTPError(400, body={'errors': [{'id': 'bad_arguments', 'message': 'Bad Arguments'}]})

        addresses = payload['addresses']

        for address in addresses:
            if not validate_address(address):
                raise JSONHTTPError(400, body={'errors': [{'id': 'bad_arguments', 'message': 'Bad Arguments'}]})

        async with self.db:

            await self.db.execute(
                "DELETE FROM notification_registrations WHERE service != 'ws' AND toshi_id = $1 AND ({})".format(
                    ' OR '.join('eth_address = ${}'.format(i + 2) for i, _ in enumerate(addresses))),
                toshi_id, *addresses)

            await self.db.commit()

        self.set_status(204)
        self.track(toshi_id, "Deregistered ETH notifications")
