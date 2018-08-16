import spongeblob as sb
from tenacity import (Retrying,
                      retry_if_exception_type,
                      stop_after_attempt,
                      wait_exponential)
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class RetriableStorage:
    """This class wraps the spongeblob storage client with tenacity library for
    retries.
    """
    # NOTE: tenacity library can only wrap regular functions for retries and
    # can't wrap generators. That's why skipping `list_object_keys` function
    # from retry method list
    RETRIABLE_METHODS = set([
        "download_file",
        "list_object_keys_flat",
        "upload_file",
        "upload_file_obj",
        "copy_from_key",
        "delete_key"])

    def __init__(self, provider,
                 max_attempts=3, wait_multiplier=2, max_wait_seconds=30,
                 *args, **kwargs):
        """Intitialize a storage service which retries for `max_attempts` with
           exponential backoff after each attempt. After each attempt, backoff
           time equals 2 ^ attempt_number * `wait_multiplier`. This value will
           increase upto `max_wait_seconds`. Read tenacity docs for
           `wait_exponential` function for more details

        :param str provider: Any provider supported by spongeblob
        :param int max_attempts: Maximum retry attempts
        :param int wait_multiplier: Multiplier factor for wait_exponential backoff function
        :param int max_wait_seconds: Max wait time between attempts

        """
        self._storage = sb.setup_storage(provider, *args, **kwargs)
        # create a cache for retrying objects
        self._retry_cache = {}
        self.retrying_args = {
            'reraise': True,
            'stop': stop_after_attempt(max_attempts),
            # Log if more than 1 attempt required
            'before': lambda x, y: None if y < 2 else
            logger.warn("Attempt {0} for running function {1}".format(y, x)),
            'wait': wait_exponential(multiplier=wait_multiplier,
                                     max=max_wait_seconds)
        }

        # collect callable methods in _storage
        self.callable_methods = set([method for method in
                                     dir(self._storage)
                                     if callable(getattr(self._storage,
                                                         method))])

    def __getattr__(self, attr):
        if attr in (RetriableStorage.RETRIABLE_METHODS &
                    self.callable_methods):
            if attr not in self._retry_cache:
                self._retry_cache[attr] = Retrying(
                    retry=retry_if_exception_type(
                        self._storage.get_retriable_exceptions(attr)),
                    **self.retrying_args
                )

            def wrapper(*args, **kwargs):
                retry = self._retry_cache[attr]
                return retry.call(getattr(self._storage, attr),
                                  *args, **kwargs)
            return wrapper
        elif attr in self.callable_methods:
            return getattr(self._storage, attr)
        else:
            raise AttributeError

    def __repr__(self):
        return "RetriableStorage({0})".format(self._storage)

    def __str__(self):
        return self.__repr__()
