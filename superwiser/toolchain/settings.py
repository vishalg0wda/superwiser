from os.path import join

from superwiser.common.settings import CONF_DIR

MAIN_CONF_PATH = join(CONF_DIR, "supervisord.conf")
INCLUDE_CONF_PATH = join(CONF_DIR, "magic.ini")
