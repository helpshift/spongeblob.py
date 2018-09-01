from sys import modules

from .storage import *


def setup_storage(storage_provider, *args, **kwargs):
    """Function to setup a Storage object for specified storage provider

    :param str storage_provider: Setup storage with specified storage provider.
                                 Supported storage_provider are 's3' and 'wabs'.
    :param \**kwargs: For the storage provider used, you will also be required to pass
                      initialization parameters of the respective storage class.
    :returns: An object for the specified storage class setup with passed storage creds
    :rtype: S3, WABS
    :Example:
        ::

            from spongeblob import setup_storage

            s3 = setup_storage('s3',
                               aws_key='access_key_id',
                               aws_secret='access_key_secret',
                               bucket_name='testbucket')

            wabs = setup_storage('wabs',
                                 account_name='testaccount',
                                 container_name='testcontainer',
                                 sas_token='testtoken')

    """
    try:
        storage_class = getattr(modules[__name__],
                                storage_provider.upper())
        return storage_class(*args, **kwargs)
    except AttributeError:
        raise ValueError('Unsupported storage "{0}"'.format(storage_provider))
