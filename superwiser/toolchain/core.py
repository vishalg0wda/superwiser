from kazoo.client import KazooClient
from kazoo.recipe.watchers import DataWatch
from kazoo.protocol.states import EventType

from superwiser.common.path import PathMaker
from superwiser.common.log import logger
from superwiser.toolchain.utils import NameGenerator


class Orc(object):
    def __init__(self, host, port, supervisor, orc_host):
        self.zk = KazooClient('{}:{}'.format(host, port))
        self.path = PathMaker()
        self.name_gen = NameGenerator()
        self.name = None
        self.supervisor = supervisor
        self.orc_host = orc_host
        self.setup()

    def setup_nodes(self):
        # Setup ephemeral nodes
        lock = self.zk.Lock(self.path.namelock())
        with lock:
            used_names = self.zk.get_children(self.path.toolchain())
            new_name = self.name_gen.generate()
            while new_name in used_names:
                new_name = self.name_gen.generate()
            self.name = new_name
            # Register watch
            DataWatch(
                self.zk,
                self.path.toolchain(self.name),
                self.on_sync)
            # Setup path for conf synchronization
            self.zk.create(
                self.path.toolchain(new_name),
                ephemeral=True)
        # Put information about node
        self.zk.create(
            self.path.node(self.name),
            value=self.orc_host,
            ephemeral=False)

    def setup(self):
        logger.info('Setting up Orc')
        self.zk.start()
        # Setup nodes
        self.setup_nodes()

    def on_sync(self, data, stat, event):
        if event and event.type == EventType.CHANGED:
            logger.info('Synchronizing toolchain')
            self.supervisor.update(data)

    def teardown(self):
        logger.info('Tearing down Orc')
        self.zk.stop()
        self.zk.close()
        self.supervisor.teardown()
