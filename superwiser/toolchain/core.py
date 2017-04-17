from kazoo.client import KazooClient
from kazoo.recipe.watchers import DataWatch
from kazoo.protocol.states import EventType

from superwiser.common.path import PathMaker
from superwiser.common.log import logger


class Orc(object):
    def __init__(self, host, port, supervisor, orc_host):
        self.zk = KazooClient('{}:{}'.format(host, port))
        self.path = PathMaker()
        self.supervisor = supervisor
        self.orc_host = orc_host
        self.setup()

    def setup(self):
        logger.info('Setting up Orc')
        self.zk.start()

        # Setup ephemeral node
        orc_conf_path = self.path.toolchain('orc')
        path = self.zk.create(
            orc_conf_path,
            sequence=True,
            ephemeral=True)

        # Put information about node
        orc_node_path = self.path.node(path.split('/')[-1])
        print orc_node_path
        self.zk.create(
            orc_node_path,
            value=self.orc_host,
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
