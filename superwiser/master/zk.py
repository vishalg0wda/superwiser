from kazoo.client import KazooClient

from superwiser.settings import ZK_HOST, ZK_PORT, SERVICE_NAMESPACE
from superwiser.common.parser import parse_content
from superwiser.common.log import logger
from superwiser.master.core import WNode
from superwiser.master.factory import BaseConfFactory, DistributorFactory


class PathMaker(object):
    def node(self, node_name=None):
        path = "/{}/wnode".format(SERVICE_NAMESPACE)
        if node_name is not None:
            path = "{}/{}".format(path, node_name)
        return path

    def nsync(self, node_name=None):
        path = "{}/sync".format(self.node())
        if node_name is not None:
            path = "{}/{}".format(path, node_name)
        return path

    def ncurrent(self, node_name=None):
        path = "{}/current".format(self.node())
        if node_name is not None:
            path = "{}/{}".format(path, node_name)
        return path

    def master(self):
        return "{}/master".format(SERVICE_NAMESPACE)

    def baseconf(self):
        return "{}/conf-base".format(self.master())

    def stateconf(self):
        return "{}/conf-state".format(self.master())


class ZkClient(object):
    def __init__(self):
        self.con = KazooClient('{}:{}'.format(ZK_HOST, ZK_PORT))
        self.con.start()
        self.path = PathMaker()
        self.distributor = DistributorFactory().make_distributor()
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

    def init_base_conf(self):
        conf, stat = self.con.get(self.path.baseconf())
        bc = BaseConfFactory().make_base_conf()
        bc.set_parsed(parse_content(conf))

    def init_wnodes(self):
        d = DistributorFactory().make_distributor()
        # Walk over current nodes and setup distributor
        for node_name in self.con.get_children(self.path.ncurrent()):
            conf = self.con.get(self.path.ncurrent(node_name))
            d.add_node(
                WNode(
                    node_name,
                    parse_content(conf),
                    d.base_conf))

    def teardown(self):
        logger.info('tearing down zk client')
