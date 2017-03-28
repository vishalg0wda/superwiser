import optparse

from twisted.internet import reactor

from superwiser.common.log import logger
from superwiser.toolchain.factory import OrcFactory


# Read conf path from command line
def parse_opts():
    parser = optparse.OptionParser()
    parser.add_option('-c', '--config', dest='conf_path')
    parser.add_option('--zk-host')
    parser.add_option('--zk-port')
    options, _ = parser.parse_args()
    return options.__dict__


def start_loop():
    orc = OrcFactory().make_orc(**parse_opts())
    reactor.addSystemEventTrigger('before', 'shutdown', orc.teardown)
    logger.info('starting main loop')
    reactor.run()


if __name__ == '__main__':
    start_loop()
