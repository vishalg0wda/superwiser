from setuptools import setup

setup(name='he-superwiser-master',
      version='0.1',
      description='Master to control jobs across distributed supervisors',
      url='https://github.com/farthVader91/superwiser/',
      author='Vishal Gowda',
      author_email='cartmanboy1991@gmail.com',
      license='GPL',
      install_requires=[
          'kazoo==2.2.1',
          'requests==2.13.0',
          'supervisor==3.3.1',
          'Twisted==17.1.0',
      ],
      packages=[
          'superwiser',
          'superwiser.common',
          'superwiser.master',
      ],
      entry_points={
        'console_scripts': [
            'saurond = superwiser.master.server:start_server'],
      },
      zip_safe=False)
