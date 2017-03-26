from kazoo.client import KazooClient
from kazoo.protocol.states import EventType

from superwiser.settings import ZK_HOST, ZK_PORT
from superwiser.common.path import PathMaker
from superwiser.common.log import logger
from superwiser.node.utils import write_to_conf, update_supervisor, teardown


class ZkClient(object):
    def __init__(self):
        self.con = KazooClient('{}:{}'.format(ZK_HOST, ZK_PORT))
        self.con.start()
        self.path = PathMaker()
        self.name = None
        self.init_nodes()

    def init_nodes(self):
        # first setup sequential, ephemeral node
        path = self.con.create(
            self.path.node('wnode'), ephemeral=True, sequence=True)
        self.name = path.split('/')[-1]
        # setup sync and current nodes
        sync_path = self.path.nsync(self.name)
        cur_path = self.path.ncurrent(self.name)
        self.con.create(sync_path, ephemeral=True)
        self.con.create(cur_path)
        # register watch on sync node.
        self.con.DataWatch(sync_path, self.on_sync)

    def on_sync(self, data, stat, event):
        if event and event.type == EventType.CHANGED:
            # Prevent dirty current conf reads
            cur_path = self.path.ncurrent(self.name)
            lock = self.con.Lock(cur_path, identifier="node-sync")
            with lock:
                self.synchronize(data)
                self.con.set(cur_path, data)

    def synchronize(self, conf):
        write_to_conf(conf)
        return update_supervisor()

    def teardown(self):
        logger.info('tearing down zookeeper client')
        teardown()
        self.con.stop()
        self.con.close()
