from superwiser.master.core import Distributor, EyeOfMordor, BaseConf


class BaseConfFactory(object):
    _instance = None

    def make_base_conf(self):
        if BaseConfFactory._instance is None:
            BaseConfFactory._instance = BaseConf()
        return BaseConfFactory._instance


class DistributorFactory(object):
    _instance = None

    def make_distributor(self):
        if DistributorFactory._instance is None:
            base_conf = BaseConfFactory().make_base_conf()
            DistributorFactory._instance = Distributor(base_conf)
        return DistributorFactory._instance


class EyeOfMordorFactory(object):
    _instance = None

    def make_eye_of_mordor(self):
        if EyeOfMordorFactory._instance is None:
            base_conf = BaseConfFactory().make_base_conf()
            EyeOfMordorFactory._instance = EyeOfMordor(
                base_conf,
                DistributorFactory().make_distributor())
        return EyeOfMordorFactory._instance


from superwiser.master.zk import ZkClient


class ZkClientFactory(object):
    _instance = None

    def make_zk_client(self):
        if ZkClientFactory._instance is None:
            ZkClientFactory._instance = ZkClient()
        return ZkClientFactory._instance
