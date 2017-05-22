import optparse

from twisted.internet import reactor
from yaml import load

from superwiser.master.core import SauronFactory
from superwiser.master.endpoints import SuperwiserWebFactory
from superwiser.master.endpoints import SuperwiserTCPFactory
from superwiser.common.log import logger


def parse_opts():
    parser = optparse.OptionParser()
    parser.add_option('--conf', dest='master_conf')
    parser.add_option('--zk-host', dest='zookeeper_host',
                      default='localhost')
    parser.add_option('--zk-port', dest='zookeeper_port',
                      type='int', default=2181)
    parser.add_option('--tcp-port', dest='master_tcp_port',
                      type='int', default=8081)
    parser.add_option('--web-port', dest='master_web_port',
                      type='int', default=8080)
    parser.add_option('--auto-redistribute',
                      action='store_true',
                      default=False,
                      dest='auto_redistribute_on_failure')
    parser.add_option('--override-state',
                      default=False,
                      action='store_true')
    parser.add_option('--supervisor-conf')
    parser.add_option('--supervisor-poll-interval',
                      type='int', default=15)
    (options, _) = parser.parse_args()
    return options


def build_conf(options):
    # Initializse conf
    conf = {}
    # Extract master conf
    master_conf = options.master_conf
    if master_conf:
        conf = load(open(master_conf))
    # Apply specified parameters as overrides
    overrides = {k: v for k, v in options.__dict__.items() if k not in conf}
    conf.update(overrides)
    if conf['supervisor_conf'] is None:
        raise Exception('Supervisor conf not provided')
    return conf


def start_server():
    options = parse_opts()
    conf = build_conf(options)

    sauron = SauronFactory().make_sauron(**conf)
    # Register TCP listener
    reactor.listenTCP(
        conf['master_tcp_port'],
        SuperwiserTCPFactory(sauron))

    # Register Web listener
    logger.info('Adding web interface')
    reactor.listenTCP(
        conf['master_web_port'],
        SuperwiserWebFactory().make_site(sauron))

    # Register teardown function
    reactor.addSystemEventTrigger('before', 'shutdown', sauron.teardown)

    logger.info('Starting server now')

    reactor.run()
