from superwiser.settings import SERVICE_NAMESPACE


class PathMaker(object):
    def namespace(self):
        return "/{}".format(SERVICE_NAMESPACE)

    def toolchain(self, name=None):
        path = "{}/toolchains".format(self.namespace())
        if name is not None:
            path = "{}/{}".format(path, name)
        return path

    def conf(self):
        return "{}/conf".format(self.namespace())
