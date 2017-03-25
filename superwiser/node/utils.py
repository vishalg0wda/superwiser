import os
import subprocess

from superwiser.node.settings import CONF_DIR, INCLUDE_CONF_PATH
from superwiser.node.settings import MAIN_CONF_PATH
from superwiser.common.parser import parse_content
from superwiser.common.log import logger


def initialize_dirs():
    if not os.path.exists(CONF_DIR):
        os.makedirs(CONF_DIR)


def is_supervisor_running():
    cmd = ['supervisorctl', '-c', MAIN_CONF_PATH, 'pid']
    output = subprocess.check_output(cmd)
    try:
        # returns a pid if running
        int(output)
        return True
    except ValueError:
        return False


def start_supervisor():
    logger.info('starting supervisor')
    cmd = ['supervisord', '-c', MAIN_CONF_PATH]
    return subprocess.check_call(cmd)


def stop_supervisor():
    logger.info('stopping supervisor')
    cmd = ['supervisorctl', '-c', MAIN_CONF_PATH, 'shutdown']
    return subprocess.check_call(cmd)


def update_supervisor():
    logger.info('updating supervisor')
    cmd = ['supervisorctl', '-c', MAIN_CONF_PATH, 'update']
    return subprocess.check_call(cmd)


def write_to_conf(conf):
    logger.info('writing conf')
    with open(INCLUDE_CONF_PATH, 'w') as dest:
        parse_content(conf).write(dest)


def teardown():
    logger.info('tearing down')
    stop_supervisor()


def get_current_conf():
    content = ''
    if os.path.exists(INCLUDE_CONF_PATH):
        with open(INCLUDE_CONF_PATH) as source:
            content = source.read()

    return content
