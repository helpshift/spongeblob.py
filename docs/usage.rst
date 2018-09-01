Usage
=====

Setting Up Storage Client
-------------------------

`setup_storage` is function to initialize a storage client for required cloud
storage provider. Refer example on how to create s3/wabs client respectively.

.. py:currentmodule:: spongeblob
.. autofunction:: setup_storage


S3
---

S3 class implements the :py:class:`spongeblob.storage.storage.Storage` class
interface documented below for AWS Simple Storage Service. You can use the
parameters documented here for a s3 client initialization via
:py:func:`setup_storage`


.. py:currentmodule:: spongeblob.storage.s3
.. autoclass:: S3

   .. automethod:: __init__

WABS
----

WABS class implements the :py:class:`spongeblob.storage.storage.Storage` class
interface documented below for Windows Azure Blob Storage service. You can use
the parameters documented here for a wabs client initialization via
:py:func:`setup_storage`

.. py:currentmodule:: spongeblob.storage.wabs
.. autoclass:: WABS

   .. automethod:: __init__


Storage API
-----------

All supported storage providers in spongeblob implement the storage class for
interface. Refer this document for the API available for
:py:class:`spongeblob.storage.s3.S3` and
:py:class:`spongeblob.storage.wabs.WABS` classes


.. py:currentmodule:: spongeblob.storage.storage

.. autoclass:: Storage
   :members:
   :exclude-members: get_retriable_exceptions


Retriable Storage
-----------------

Cloud storage api calls fail randomly due to various issues like timeouts.
Handling retries is a common task. In spongeblob, both S3 and WABS classes
implement a `get_retriable_excpetions` method which takes the method name as
parameter and returns the exceptions which should be generally retried for it.
:py:class:`spongeblob.retriable_storage.RetriableStorage` wraps the S3/WABS
client above and retries for acceptable exceptions for each method via
`tenacity` library. This is just one approach to retry exceptions, and you can
use it if it fits your requirement. All storage class api work directly with
retriable storage as well, once the client is initialized.


.. py:currentmodule:: spongeblob.retriable_storage

.. autoclass:: RetriableStorage

   .. automethod:: __init__
