from twisted.internet import reactor

from superwiser.common.log import logger
from superwiser.toolchain.core import Orc
from superwiser.toolchain.utils import Supervisor, Conf
from superwiser.toolchain.settings import MAIN_CONF_PATH, INCLUDE_CONF_PATH


def setup_orc():
    logger.info('Settings up orc')
    # setup Orc
    conf = Conf(MAIN_CONF_PATH, INCLUDE_CONF_PATH)
    supervisor = Supervisor(conf)
    orc = Orc(supervisor)
    return orc


def start_loop():
    orc = setup_orc()
    reactor.addSystemEventTrigger('before', 'shutdown', orc.teardown)
    logger.info('starting main loop')
    reactor.run()


if __name__ == '__main__':
    start_loop()
