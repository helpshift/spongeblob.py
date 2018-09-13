from setuptools import setup, find_packages

from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md')) as f:
    long_description = f.read()

setup(name='spongeblob',
      version='0.1.1',
      description='Spongeblob: A wrapper library for various cloud storage',
      long_description=long_description,
      long_description_content_type='text/markdown',
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
