import datetime

from twisted.web.server import Site
from twisted.web.resource import Resource

from superwiser.common.jinja_manager import JinjaTemplateManager


class SuperwiserHome(Resource):
    template_manager = JinjaTemplateManager()
    isLeaf = True

    def render_GET(self, request):
        context = {'time': str(datetime.datetime.now())}
        return self.template_manager.render_template('status_page.html',
                                                     context)


def get_site_root():
    root = SuperwiserHome()
    site = Site(root)
    return site
