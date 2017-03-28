from kazoo.client import KazooClient
from kazoo.recipe.watchers import DataWatch
from kazoo.protocol.states import EventType

from superwiser.common.path import PathMaker
from superwiser.common.log import logger


class Orc(object):
    def __init__(self, host, port, supervisor):
        self.zk = KazooClient('{}:{}'.format(host, port))
        self.path = PathMaker()
        self.supervisor = supervisor
        self.setup()

    def setup(self):
        logger.info('Setting up Orc')
        self.zk.start()
        # Setup ephemeral node
        path = self.zk.create(
            self.path.toolchain('orc'),
            sequence=True,
            ephemeral=True)
        # Register watch
        DataWatch(
            self.zk,
            path,
            self.on_sync)

    def on_sync(self, data, stat, event):
        if event and event.type == EventType.CHANGED:
            logger.info('Synchronizing toolchain')
            self.supervisor.update(data)

    def teardown(self):
        logger.info('Tearing down Orc')
        self.zk.stop()
        self.zk.close()
        self.supervisor.teardown()
