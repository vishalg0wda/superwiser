from superwiser.master.core import Distributor, EyeOfMordor, BaseConf


class BaseConfFactory(object):
    _instance = None

    def make_base_conf(self, parsed):
        if BaseConfFactory._instance is None:
            BaseConfFactory._instance = BaseConf(parsed)
        return BaseConfFactory._instance


class DistributorFactory(object):
    _instance = None

    def make_distributor(self, base_conf):
        if DistributorFactory._instance is None:
            DistributorFactory._instance = Distributor(base_conf)
        return DistributorFactory._instance


class EyeOfMordorFactory(object):
    _instance = None

    def make_eye_of_mordor(self, base_conf):
        if EyeOfMordorFactory._instance is None:
            EyeOfMordorFactory._instance = EyeOfMordor(
                base_conf,
                DistributorFactory().make_distributor(base_conf))
        return EyeOfMordorFactory._instance
