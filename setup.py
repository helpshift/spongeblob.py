from setuptools import setup, find_packages

setup(name='spongeblob',
      version='0.0.1',
      description='Spongeblob: A wrapper library for various cloud storage',
      url='',
      license='Proprietary',
      packages=find_packages(),
      install_requires=['azure-storage-blob==1.1.0',
                        'boto3==1.7.12'],
      tests_require=['pytest',
                     'pytest-docker'],
      test_suite='pytest'
      )
