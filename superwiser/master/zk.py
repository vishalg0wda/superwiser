from kazoo.client import KazooClient
from kazoo.protocol.states import EventType

from superwiser.settings import ZK_HOST, ZK_PORT
from superwiser.master.settings import AUTO_REDISTRIBUTE
from superwiser.common.parser import parse_content, build_conf, merge_confs
from superwiser.common.parser import extract_conf_from_parsed
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
                          self.path.stateconf(),
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

    def on_wnode(self, children, event):
        if not event:
            return
        children = set(children) - {'current', 'sync'}
        removed = set(self.wnodes) - set(children)
        added = set(children) - set(self.wnodes)
        state_conf = parse_content(self.con.get(self.path.stateconf())[0])

        import ipdb; ipdb.set_trace()

        for node in removed:
            self.distributor.remove_node(node)
            self.wnodes.remove(node)
            data, stat = self.con.get(self.path.ncurrent(node))
            conf = parse_content(data)
            if AUTO_REDISTRIBUTE:
                # get conf and redistribute across the rest
                self.distributor.distribute_conf(conf)
            # remove current node
            self.con.delete(self.path.ncurrent(node))
            state_conf = build_conf(
                merge_confs(
                    state_conf,
                    conf,
                    removed=True))
        for node in added:
            # register watch on current node
            self.con.DataWatch(self.path.ncurrent(node), self.on_ncurrent)
            data, stat = self.con.get(self.path.ncurrent(node))
            wnode = WNode(
                node, parse_content(data), self.distributor.base_conf)
            wnode.is_dirty = True
            self.distributor.add_node(wnode)
            self.wnodes.append(node)
            state_conf = build_conf(
                merge_confs(state_conf, wnode.parsed),
                self.distributor.base_conf.parsed)
        if children:
            self.distributor.distribute()

        self.set_state_conf(extract_conf_from_parsed(state_conf))
        self.distributor.synchronize_nodes(self.sync_node)

    def on_ncurrent(self, data, stat, event):
        if event and event.type == EventType.CHANGED:
            logger.info('Supervisor updated for a node')
            state_conf = parse_content(self.con.get(self.path.stateconf())[0])
            state_conf = build_conf(
                merge_confs(state_conf, parse_content(data)),
                self.distributor.base_conf.parsed)
            self.set_state_conf(extract_conf_from_parsed(state_conf))

    def init_base_conf(self):
        conf, stat = self.con.get(self.path.baseconf())
        self.distributor.base_conf.set_parsed(parse_content(conf))

    def init_wnodes(self):
        # setup watches
        self.con.ChildrenWatch(
            self.path.node(), self.on_wnode, send_event=True)
        state_conf = parse_content('')
        # Walk over current nodes
        for node_name in self.con.get_children(self.path.ncurrent()):
            self.wnodes.append(node_name)
            conf, stat = self.con.get(self.path.ncurrent(node_name))
            # setup distributor
            node = WNode(
                node_name,
                parse_content(conf),
                self.distributor.base_conf)
            self.distributor.add_node(node)
            # assemble state conf
            state_conf = build_conf(
                merge_confs(state_conf, node.parsed),
                self.distributor.base_conf.parsed)
        # set state conf
        self.set_state_conf(extract_conf_from_parsed(state_conf))

    def set_base_conf(self, conf):
        self.con.set(self.path.baseconf(), conf)

    def set_state_conf(self, conf):
        self.con.set(self.path.stateconf(), conf)

    def sync_node(self, node, conf):
        logger.info('synchronizing node')
        sync_path = self.path.nsync(node.name)
        self.con.set(sync_path, conf)

    def teardown(self):
        logger.info('tearing down zk client')
        self.con.stop()
        self.con.close()
