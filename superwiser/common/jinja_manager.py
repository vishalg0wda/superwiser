from jinja2 import Environment, PackageLoader, select_autoescape


class JinjaTemplateManager(object):
    def __init__(self):
        self.env = Environment(
            loader=PackageLoader('superwiser.master', 'templates'),
            autoescape=select_autoescape(['html', 'xml'])
        )

    def render_template(self, template_name, context):
        template = self.env.get_template(template_name)
        return template.render(context).encode('utf-8')
