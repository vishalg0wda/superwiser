from setuptools import setup

setup(name='he-superwiser',
      version='0.1',
      description='Distributed job control across supervisors',
      url='https://github.com/farthVader91/superwiser/',
      author='Vishal Gowda',
      author_email='cartmanboy1991@gmail.com',
      license='GPL',
      install_requires=[
          'kazoo==2.2.1',
          'supervisor==3.3.1',
          'Twisted==17.1.0',
      ],
      packages=[
          'superwiser',
          'superwiser.common',
          'superwiser.toolchain',
      ],
      entry_points={
        'console_scripts': [
            'superwiserd = superwiser.toolchain.mainloop:start_loop'],
      },
      package_data={
          'superwiser': ['conf/supervisord.conf', 'conf/magic.ini'],
      },
      zip_safe=False)
