import random
import shutil
import subprocess
from os import path

from superwiser.common.log import logger
from superwiser.toolchain.settings import CONF_TEMPLATE
from superwiser.toolchain.constants import ADJECTIVES, ORC_NAMES


class Conf(object):
    def __init__(self, main_path):
        self.main_path = main_path
        self.include_path = path.join(path.dirname(main_path), 'magic.ini')
        self.setup()

    @staticmethod
    def create(conf_path):
        if not path.exists(conf_path):
            logger.info('Generating conf from template')
            # Copy main conf
            shutil.copyfile(CONF_TEMPLATE, conf_path)
            # Touch magic conf
            open(path.join(path.dirname(conf_path), 'magic.ini'), 'w').close()
        return Conf(conf_path)

    def setup(self):
        assert path.exists(self.main_path), "Conf does not exist!"
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
        self.conf_path = conf.main_path
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

    def update(self, conf):
        logger.info('updating supervisor')
        self.conf.write(conf)
        cmd = ['supervisorctl', '-c', self.conf_path, 'update']
        return subprocess.check_call(cmd)

    def teardown(self):
        logger.info('tearing down')
        self.stop()


class NameGenerator(object):
    LEFT = ADJECTIVES
    RIGHT = ORC_NAMES

    def generate(self):
        name = "{} {}".format(
            random.choice(self.LEFT), random.choice(self.RIGHT))
        return name.title()
