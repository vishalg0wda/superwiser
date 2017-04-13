import os

from superwiser.common.log import logger
from superwiser.master.core import EyeOfMordor, Sauron


class SauronFactory(object):
    _instance = None

    @classmethod
    def make_sauron(self, **kwargs):
        if self._instance:
            return self._instance

        logger.info('Making Sauron')
        supervisor_conf = kwargs.get('superwiser_conf', None)
        zookeeper_host = kwargs.get('zookeeper_host')
        if zookeeper_host is None:
            zookeeper_host = os.environ.get('ZK_HOST', 'localhost')
        zookeeper_port = kwargs.get('zookeeper_port')
        if zookeeper_port is None:
            zookeeper_port = os.environ.get('ZK_PORT', 2181)
        auto_redistribute_on_failure = kwargs.get(
            'auto_redistribute_on_failure')
        if auto_redistribute_on_failure is None:
            auto_redistribute_on_failure = False

        if supervisor_conf is None:
            supervisor_conf = os.path.join(os.getcwd(), 'aggregate.conf')
        if not os.path.exists(supervisor_conf):
            open(supervisor_conf, 'w').close()

        self._instance = Sauron(
            supervisor_conf,
            EyeOfMordor(zookeeper_host, zookeeper_port,
                        auto_redistribute_on_failure))
        return self._instance

    @classmethod
    def get_sauron(self):
        return self._instance
