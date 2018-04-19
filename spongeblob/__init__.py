from sys import modules

from .storage import *


def setup_storage(storage_provider, *args, **kwargs):
    try:
        storage_class = getattr(modules[__name__],
                                storage_provider.upper())
        return storage_class(*args, **kwargs)
    except AttributeError:
        raise ValueError('Unsupported storage "{0}"'.format(storage_provider))
