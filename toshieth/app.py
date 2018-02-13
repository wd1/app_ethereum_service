import toshi.web
import os

from . import handlers
from . import websocket
from .tasks import EthServiceTaskListener

from toshi.handlers import GenerateTimestamp
from toshi.jsonrpc.client import JsonRPCClient

from toshi.log import configure_logger
from toshi.log import log as services_log

from tornado.platform.asyncio import to_asyncio_future

urls = [
    (r"^/v1/tx/skel/?$", handlers.TransactionSkeletonHandler),
    (r"^/v1/tx/?$", handlers.SendTransactionHandler),
    (r"^/v1/tx/cancel/?$", handlers.CancelTransactionHandler),
    (r"^/v1/tx/(0x[0-9a-fA-F]{64})/?$", handlers.TransactionHandler),
    (r"^/v1/balance/(0x[0-9a-fA-F]{40})/?$", handlers.BalanceHandler),
    (r"^/v1/address/(0x[0-9a-fA-F]{40})/?$", handlers.AddressHandler),
    (r"^/v1/timestamp/?$", GenerateTimestamp),
    (r"^/v1/(apn|gcm)/register/?$", handlers.PNRegistrationHandler),
    (r"^/v1/(apn|gcm)/deregister/?$", handlers.PNDeregistrationHandler),
    (r"^/v1/ws/?$", websocket.WebsocketHandler),

    (r"^/v1/gasprice/?$", handlers.GasPriceHandler),

    # legacy
    (r"^/v1/register/?$", handlers.LegacyRegistrationHandler),
    (r"^/v1/deregister/?$", handlers.LegacyDeregistrationHandler),

    # (essentially) static file access
    (r"^/tokens/?$", handlers.TokenHandler),
    (r"^/token/(?P<symbol_png>.+\.png)$", handlers.TokenHandler),

    # status
    (r"^/v1/status/?$", handlers.StatusHandler)
]

class Application(toshi.web.Application):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.task_listener = EthServiceTaskListener(self)

    def process_config(self):
        config = super().process_config()

        if 'ETHEREUM_NODE_URL' in os.environ:
            config['ethereum'] = {'url': os.environ['ETHEREUM_NODE_URL']}

        if 'DEFAULT_GASPRICE' in os.environ:
            if 'ethereum' not in config:
                config['ethereum'] = {}
            config['ethereum']['default_gasprice'] = os.environ['DEFAULT_GASPRICE']

        if 'ethereum' in config:
            if 'ETHEREUM_NETWORK_ID' in os.environ:
                config['ethereum']['network_id'] = os.environ['ETHEREUM_NETWORK_ID']
            else:
                config['ethereum']['network_id'] = self.asyncio_loop.run_until_complete(
                    to_asyncio_future(JsonRPCClient(config['ethereum']['url']).net_version()))

        configure_logger(services_log)

        return config

    def start(self):
        self.task_listener.start_task_listener()
        super().start()

def main():
    app = Application(urls)
    app.start()
