import logging
import os
from configparser import SectionProxy

from toshi.tasks import TaskHandler
from toshi.database import DatabaseMixin
from toshi.log import configure_logger
from toshi.push import PushServerClient, GCMHttpPushClient

from toshieth.tasks import TaskListenerApplication

log = logging.getLogger("toshieth.push_service")

class PushNotificationHandler(DatabaseMixin, TaskHandler):

    def initialize(self, pushclient):
        self.pushclient = pushclient

    async def send_notification(self, eth_address, message):

        async with self.db:
            rows = await self.db.fetch("SELECT * FROM notification_registrations WHERE eth_address = $1",
                                       eth_address)

        for row in rows:
            service = row['service']
            if service in ['gcm', 'apn']:
                log.debug("Sending {} PN to: {} ({})".format(service, eth_address, row['registration_id']))
                await self.pushclient.send(row['toshi_id'], service, row['registration_id'], {"message": message})

class PushNotificationService(TaskListenerApplication):

    def __init__(self, pushclient=None, **kwargs):

        super().__init__([],
                         listener_id="pushnotification",
                         **kwargs)

        if pushclient is not None:
            self.pushclient = pushclient
        elif 'gcm' in self.config and 'server_key' in self.config['gcm'] and self.config['gcm']['server_key'] is not None:
            self.pushclient = GCMHttpPushClient(self.config['gcm']['server_key'])
        elif 'pushserver' in self.config and 'url' in self.config['pushserver'] and self.config['pushserver']['url'] is not None:
            self.pushclient = PushServerClient(url=self.config['pushserver']['url'],
                                               username=self.config['pushserver'].get('username'),
                                               password=self.config['pushserver'].get('password'))
        else:
            raise Exception("Unable to find appropriate push notification client config")

        self.task_listener.add_task_handler(PushNotificationHandler, {'pushclient': self.pushclient})

    def process_config(self):
        config = super().process_config()

        if 'PUSH_URL' in os.environ:
            config.setdefault('pushserver', SectionProxy(config, 'pushserver'))['url'] = os.environ['PUSH_URL']
        if 'PUSH_PASSWORD' in os.environ:
            config.setdefault('pushserver', SectionProxy(config, 'pushserver'))['password'] = os.environ['PUSH_PASSWORD']
        if 'PUSH_USERNAME' in os.environ:
            config.setdefault('pushserver', SectionProxy(config, 'pushserver'))['username'] = os.environ['PUSH_USERNAME']

        if 'GCM_SERVER_KEY' in os.environ:
            config.setdefault('gcm', SectionProxy(config, 'gcm'))['server_key'] = os.environ['GCM_SERVER_KEY']

        configure_logger(log)
        return config


if __name__ == "__main__":
    app = PushNotificationService()
    app.run()
