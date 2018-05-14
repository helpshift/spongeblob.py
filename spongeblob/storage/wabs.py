import time
import logging

from .storage import Storage
from azure.common import AzureConflictHttpError, AzureException
from azure.storage.blob import BlockBlobService
from azure.storage.blob.models import Include

logger = logging.getLogger(__name__)


class WABS(Storage):
    """
    A class for managing objects on Windows Azure Blob Storage. It implements
    the interface of Storage base class
    """

    def __init__(self, account_name, container_name, sas_token):
        """Setup a Windows azure blob storage client object

        :param str account_name: Azure blob storage account name for connection
        :param str container_name: Name of container to be accessed in the account
        :param str sas_token: Shared access signature token for access

        """
        self.sas_token = sas_token
        self.container_name = container_name

        # The socket_timeout is passed on to the requests session
        # which executes the HTTP call. Both read / connect timeouts
        # are set to 60s
        self.client = BlockBlobService(account_name=account_name,
                                       sas_token=self.sas_token)
        logger.debug("Created wabs client object: {0}".format(self.client))

    @classmethod
    def get_retriable_exceptions(cls, method_name=None):
        """Return exceptions that should be retried for specified method of class

        :param str method_name: A method of class for which retriable exceptions should be searched
        :returns: A tuple of exception class to be retried
        :rtype: tuple

        """
        return (AzureException)

    def get_url_prefix(self):
        """Returns a connection string for the client object

        :returns: Connection string for the client object
        :rtype: str

        """
        return '{}://{}/{}/'.format(self.client.protocol,
                                    self.client.primary_endpoint,
                                    self.container_name)

    def list_object_keys(self, prefix='', metadata=False, pagesize=1000):
        """List object keys matching a prefix for the WABS client

        :param str prefix: A prefix string to list objects
        :param bool metadata: If set to True, object metadata will be fetched with object. Default is False
        :param int pagesize: Maximum objects to be fetched in a single WABS api call. This is limited to upto 5000 objects in WABS
        :returns: A generator of object dictionary with key, size and last_modified keys. Metadata will be returned if set to True
        :rtype: Iterator[dict]

        """

        logger.debug("Listing files for prefix: {0}".format(prefix))
        include = Include(metadata=metadata)
        marker = None
        while True:
            if marker:
                logger.debug("Paging objects "
                             "from marker '{0}'".format(marker))
            objects = self.client.list_blobs(self.container_name,
                                             prefix=prefix,
                                             num_results=pagesize,
                                             include=include,
                                             marker=marker)
            for obj in objects:
                yield {'key': obj.name,
                       'last_modified': obj.properties.last_modified,
                       'size': obj.properties.content_length,
                       'metadata': obj.metadata}

            if objects.next_marker:
                marker = objects.next_marker
            else:
                break

    def download_file(self, source_key, destination_file):
        """Download a object from WABS container to local filesystem

        :param str source_key: Key for object to be downloaded
        :param str destination_file: Path on local filesystem to download file
        :returns: Nothing
        :rtype: None

        """
        self.client.get_blob_to_path(self.container_name, source_key,
                                     destination_file)

    def upload_file(self, destination_key, source_file, metadata=None):
        """Upload a file from local filesystem to WABS

        :param str destination_key: Key where to store object
        :param str source_file: Path on local file system for file to be uploaded
        :param dict metadata: Metadata to be stored along with object
        :returns: Nothing
        :rtype: None

        """
        metadata = metadata or {}
        logger.debug("Uploading file {0} to prefix {1}"
                     .format(source_file, destination_key))
        self.client.create_blob_from_path(self.container_name, destination_key,
                                          source_file, metadata=metadata)

    def upload_file_obj(self,  destination_key, source_fd, metadata=None):
        """Upload a file from file object to WABS

        :param str destination_key: Key where to store object
        :param file source_fd: A file object to be uploaded
        :param dict metadata: Metadata to be stored along with object
        :returns: Nothing
        :rtype: None

        """
        metadata = metadata or {}
        self.client.create_blob_from_stream(self.container_name,
                                            destination_key,
                                            source_fd,
                                            metadata=metadata)

    # FIXME: Need to fix this function to abort, if another copy is already
    # happening it should abort, or it should follow the ec2 behaviour
    def copy_from_key(self, source_key, destination_key, metadata=None):
        """Copy a WABS object from one key to another key on server side

        :param str source_key: Source key for the object to be copied
        :param str destination_key: Destination key to store object
        :param dict metadata: Metadata to be stored along with object
        :returns: Nothing
        :rtype: None

        """
        metadata = metadata or {}
        logger.debug("Copying key {0} -> {1}"
                     .format(source_key, destination_key))

        # If a previous copy was pending cancel it before
        # starting another copy
        for blob in self.client.list_blobs(self.container_name,
                                           prefix=destination_key):
            # There should only be one blob with the given key,
            # However list_blobs is the only exposed API to check
            # existance of blob without failures
            # AzureBlobStorage doesn't allow more than one pending
            # copies to the destination key
            try:
                self.client.abort_copy_blob(self.container_name,
                                            destination_key,
                                            blob.properties.copy.id)
            except AzureConflictHttpError:
                logger.info(('No copy in progress,' +
                             ' Ignoring AzureConflictHttpError'))
        source_uri = self.client.make_blob_url(self.container_name,
                                               source_key,
                                               sas_token=self.sas_token)
        copy_properties = self.client.copy_blob(self.container_name,
                                                destination_key,
                                                source_uri,
                                                metadata=metadata)
        # Wait for the copy to be a success
        while copy_properties.status == 'pending':
            # Wait a second before retrying
            time.sleep(1)
            properties = self.client.get_blob_properties(self.container_name,
                                                         destination_key)
            copy_properties = properties.properties.copy
            # TODO(vin): Raise Error if copy_properties errors out

    def delete_key(self, destination_key):
        """Delete an object from WABS

        :param str destination_key: Destination key for the object to be deleted
        :returns: Nothing
        :rtype: None

        """
        logger.debug("Deleting key {0}".format(destination_key))
        return self.client.delete_blob(self.container_name, destination_key)
