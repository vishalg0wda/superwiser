import logging


logger = logging.getLogger('superwiser')
logger.setLevel(logging.DEBUG)

sh = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
sh.setFormatter(formatter)

logger.addHandler(sh)
