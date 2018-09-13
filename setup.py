from setuptools import setup, find_packages

setup(name='spongeblob',
      version='0.0.2',
      description='Spongeblob: A wrapper library for various cloud storage',
      url='https://github.com/helpshift/spongeblob.py',
      license='MIT License',
      packages=find_packages(),
      install_requires=['azure-storage-blob==1.1.0',
                        'boto3==1.7.12',
                        'tenacity==4.10.0'],
      tests_require=['pytest',
                     'pytest-docker'],
      test_suite='pytest'
      )
