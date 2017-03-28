import os

from superwiser.common.log import logger
from superwiser.master.core import EyeOfMordor, Sauron


class SauronFactory(object):
    def make_sauron(self, conf_path, zk_host, zk_port):
        logger.info('Making Sauron')
        if conf_path is None:
            conf_path = os.path.join(os.getcwd(), 'aggregate.conf')
        if not os.path.exists(conf_path):
            open(conf_path, 'w').close()
        if zk_host is None:
            zk_host = os.environ.get('ZK_HOST', 'localhost')
        if zk_port is None:
            zk_port = os.environ.get('ZK_PORT', 2181)
        return Sauron(conf_path, EyeOfMordor(zk_host, zk_port))
