from twisted.internet import reactor

from superwiser.common.log import logger
from superwiser.node.utils import init_supervisor
from superwiser.node.utils import is_supervisor_running, start_supervisor
from superwiser.node.zk import ZkClient


def initialize():
    # ensure supervisor is running
    logger.info('initializing supervisor client')
    if not is_supervisor_running():
        init_supervisor()
        start_supervisor()


def start_loop():
    initialize()
    client = ZkClient()
    logger.info('started zookeeper client')
    reactor.addSystemEventTrigger('before', 'shutdown', client.teardown)
    reactor.run()


if __name__ == '__main__':
    start_loop()
