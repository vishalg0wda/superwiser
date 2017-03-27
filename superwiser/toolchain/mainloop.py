import sys

from twisted.internet import reactor

from superwiser.common.log import logger
from superwiser.toolchain.core import Orc
from superwiser.toolchain.utils import Supervisor, Conf
from superwiser.toolchain.settings import MAIN_CONF_PATH, INCLUDE_CONF_PATH


def setup_orc():
    logger.info('Setting up orc')
    main_conf = MAIN_CONF_PATH
    inc_conf = INCLUDE_CONF_PATH
    args = sys.argv[1:]
    if args:
        main_conf = 'conf/supervisord{}.conf'.format(args[0])
        inc_conf = 'conf/magic{}.ini'.format(args[0])
    # setup Orc
    conf = Conf(main_conf, inc_conf)
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
