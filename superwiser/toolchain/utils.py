import os
import subprocess

from superwiser.node.settings import CONF_DIR
from superwiser.common.log import logger


class Conf(object):
    def __init__(self, main_path, include_path):
        self.main_path = main_path
        self.include_path = include_path
        self.setup()

    def setup(self):
        # Setup conf dir
        if not os.path.exists(CONF_DIR):
            os.makedirs(CONF_DIR)
        # Truncate conf
        self.truncate()

    def truncate(self):
        logger.info('flushing conf')
        # flush conf file
        open(self.include_path, 'w').close()

    def write(self, conf):
        logger.info('writing conf')
        with open(self.include_path, 'w') as dest:
            dest.write(conf)


class Supervisor(object):
    def __init__(self, conf):
        self.conf = conf
        self.setup()

    def setup(self):
        self.restart()

    def is_running(self):
        cmd = ['supervisorctl', '-c', self.conf_path, 'pid']
        try:
            output = subprocess.check_output(cmd)
            # returns a pid if running
            int(output)
            return True
        except (subprocess.CalledProcessError, ValueError):
            return False

    def start(self):
        logger.info('starting supervisor')
        cmd = ['supervisord', '-c', self.conf_path]
        return subprocess.check_call(cmd)

    def restart(self):
        if self.is_running():
            self.stop()
        self.start()

    def stop(self):
        logger.info('stopping supervisor')
        cmd = ['supervisorctl', '-c', self.conf_path, 'shutdown']
        return subprocess.check_call(cmd)

    def update(self):
        logger.info('updating supervisor')
        cmd = ['supervisorctl', '-c', self.conf_path, 'update']
        return subprocess.check_call(cmd)

    def teardown(self):
        logger.info('tearing down')
        self.stop()
