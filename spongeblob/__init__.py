from sys import modules

from .storage import *


def setup_storage(storage_provider, *args, **kwargs):
    """Function to setup a Storage object for specified storage provider

    :param str storage_provider: Setup storage with specified storage provider.
                                 Supported storage_provider are 's3' and 'wabs'
    :returns: An object for the specified storage class setup with passed storage creds
    :rtype: Storage

    """
    try:
        storage_class = getattr(modules[__name__],
                                storage_provider.upper())
        return storage_class(*args, **kwargs)
    except AttributeError:
        raise ValueError('Unsupported storage "{0}"'.format(storage_provider))
