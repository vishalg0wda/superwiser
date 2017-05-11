import logging


logger = logging.getLogger('superwiser')
logger.setLevel(logging.DEBUG)

sh = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
sh.setFormatter(formatter)

kazoo_logger = logging.getLogger('kazoo')

logger.addHandler(sh)
kazoo_logger.addHandler(sh)
