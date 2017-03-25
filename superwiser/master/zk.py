from kazoo.client import KazooClient
from kazoo.protocol.states import EventType

from superwiser.settings import ZK_HOST, ZK_PORT
from superwiser.master.settings import AUTO_REDISTRIBUTE
from superwiser.common.parser import parse_content
from superwiser.common.path import PathMaker
from superwiser.common.log import logger
from superwiser.master.core import WNode
from superwiser.master.factory import DistributorFactory


class ZkClient(object):
    def __init__(self):
        self.con = KazooClient('{}:{}'.format(ZK_HOST, ZK_PORT))
        self.con.start()
        self.path = PathMaker()
        self.distributor = DistributorFactory().make_distributor()
        self.wnodes = []
        self.setup_nodes()
        self.init_base_conf()
        self.init_wnodes()

    def setup_nodes(self):
        required_paths = [self.path.baseconf(),
                          self.path.nsync(),
                          self.path.ncurrent()]
        for path in required_paths:
            if not self.con.exists(path):
                self.con.ensure_path(path)
        # remove dangling current nodes
        wnodes = set(self.con.get_children(self.path.node()))
        currents = set(self.con.get_children(self.path.ncurrent()))
        for node in (currents - wnodes):
            self.con.delete(self.path.ncurrent(node))
        # setup watches
        self.con.ChildrenWatch(self.path.node(), self.on_wnode)

    def on_wnode(self, children):
        children = set(children) - {'current', 'sync'}
        removed = set(self.wnodes) - set(children)
        added = set(children) - set(self.wnodes)
        for node in removed:
            self.distributor.remove_node(node)
            self.wnodes.remove(node)
            if AUTO_REDISTRIBUTE:
                # get conf and redistribute across the rest
                data, stat = self.con.get(self.path.ncurrent(node))
                self.distributor.distribute_conf(parse_content(data))
            # remove current node
            self.con.delete(self.path.ncurrent(node))
        for node in added:
            # register watch on current node
            self.con.DataWatch(self.path.ncurrent(node), self.on_ncurrent)
            data, stat = self.con.get(self.path.ncurrent(node))
            wnode = WNode(
                node, parse_content(data), self.distributor.base_conf)
            wnode.is_dirty = True
            self.distributor.add_node(wnode)
            self.wnodes.append(node)
        if children:
            self.distributor.distribute()

        self.distributor.synchronize_nodes(self.sync_node)

    def on_ncurrent(self, data, stat, event):
        if event and event.type == EventType.CHANGED:
            logger.info('Supervisor updated for a node')

    def init_base_conf(self):
        conf, stat = self.con.get(self.path.baseconf())
        self.distributor.base_conf.set_parsed(parse_content(conf))

    def init_wnodes(self):
        # Walk over current nodes and setup distributor
        for node_name in self.con.get_children(self.path.ncurrent()):
            self.wnodes.append(node_name)
            conf, stat = self.con.get(self.path.ncurrent(node_name))
            self.distributor.add_node(
                WNode(
                    node_name,
                    parse_content(conf),
                    self.distributor.base_conf))

    def set_base_conf(self, conf):
        self.con.set(self.path.baseconf(), conf)

    def sync_node(self, node, conf):
        logger.info('synchronizing node')
        sync_path = self.path.nsync(node.name)
        self.con.set(sync_path, conf)

    def teardown(self):
        logger.info('tearing down zk client')
        self.con.stop()
        self.con.close()
