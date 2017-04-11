import os

from superwiser.common.log import logger
from superwiser.master.core import EyeOfMordor, Sauron


class SauronFactory(object):
    def make_sauron(self, supervisor_conf, zk_host,
                    zk_port, auto_redistribute):
        logger.info('Making Sauron')
        if supervisor_conf is None:
            supervisor_conf = os.path.join(os.getcwd(), 'aggregate.conf')
        if not os.path.exists(supervisor_conf):
            open(supervisor_conf, 'w').close()
        if zk_host is None:
            zk_host = os.environ.get('ZK_HOST', 'localhost')
        if zk_port is None:
            zk_port = os.environ.get('ZK_PORT', 2181)
        return Sauron(
            supervisor_conf,
            EyeOfMordor(zk_host, zk_port, auto_redistribute))
