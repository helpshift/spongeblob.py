.. spongeblob.py documentation master file, created by
   sphinx-quickstart on Sat Sep  1 11:58:14 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

spongeblob.py
=============

Overview
--------

Spongeblob is a library for providing a simple and consistent interface for
cloud storage services. Currently, the project supports AWS Simple Storage
Service(S3) and Windows Azure blob storage (WABS).

It wraps `boto` for s3 client and `azure-storage` for wabs, and provides a set
of basic operations that are commonly used.

Installation
------------
You can fetch spongeblob from pypi via pip.
::

    pip install spongeblob

Example
-------
To setup a spongeblob client use the following code:
::

    import spongeblob

    s3 = spongeblob.setup_storage('s3',
                                  aws_key='access_key_id',
                                  aws_secret='access_key_secret',
                                  bucket_name='testbucket')

    s3.download_file('/path/to/key', '/path/on/disk')


Both these clients implement methods of the spongeblob.storage class.


Contents
--------
.. toctree::
   :maxdepth: 2

   usage
