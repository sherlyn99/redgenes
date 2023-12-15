from setuptools import setup, find_packages

__version__ = '2023.12-dev'

setup(name='redgenes',
      version=__version__,
      packages=find_packages(),
      include_package_data=True)
