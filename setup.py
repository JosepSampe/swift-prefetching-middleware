from setuptools import setup, find_packages

paste_factory = ['caching = '
                 'caching:filter_factory']

setup(name='swift-caching-middleware',
      version='0.1.0',
      description='Caching middleware for OpenStack Swift',
      author='Josep Sampe',
      packages=find_packages(),
      requires=['swift(>=1.4)'],
      entry_points={'paste.filter_factory': paste_factory}
      )
