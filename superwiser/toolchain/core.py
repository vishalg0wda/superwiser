from kazoo.client import KazooClient
from kazoo.recipes.watchers import DataWatch
from kazoo.protocl.states import EventType

from superwiser.common.settings import ZK_HOST, ZK_PORT
from superwiser.common.path import PathMaker
from superwiser.common.log import logger


class Orc(object):
    def __init__(self, supervisor):
        self.zk = KazooClient('{}:{}'.format(ZK_HOST, ZK_PORT))
        self.path = PathMaker()
        self.supervisor = supervisor
        self.setup()

    def setup(self):
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
            self.supervisor.reload()
