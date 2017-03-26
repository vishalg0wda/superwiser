import os
from os.path import join

from superwiser.settings import ROOT_DIR

CONF_DIR = join(ROOT_DIR, "conf")
MAIN_CONF_PATH = join(CONF_DIR, "supervisord.conf")
INCLUDE_CONF_PATH = join(CONF_DIR, "magic.ini")
