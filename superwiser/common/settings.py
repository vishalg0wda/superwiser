from os.path import dirname, abspath, join

ROOT_DIR = dirname(dirname(dirname(abspath(__file__))))
CONF_DIR = join(ROOT_DIR, "conf")

SERVICE_NAMESPACE = 'he-superwiser'
ZK_HOST = 'localhost'
ZK_PORT = 2181
