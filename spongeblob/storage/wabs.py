import time
import logging

from .storage import Storage
from azure.common import AzureConflictHttpError, AzureException
from azure.storage.blob import BlockBlobService

logger = logging.getLogger(__name__)

class WABS(Storage):
    def __init__(self, account_name, container_name, sas_token):
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
        return (AzureException)

    def get_url_prefix(self):
        return '{}://{}/{}/'.format(self.client.protocol,
                                    self.client.primary_endpoint,
                                    self.container_name)

    def list_object_keys(self, prefix=''):
        logger.debug("Listing files for prefix: {0}".format(prefix))
        return [{'key': obj.name,
                 'last_modified': obj.properties.last_modified}
                for obj in self.client.list_blobs(self.container_name,
                                                  prefix=prefix)]

    def download_file(self, source_key, destination_file):
        self.client.get_blob_to_path(self.container_name, source_key,
                                     destination_file)

    def upload_file(self, destination_key, source_file):
        logger.debug("Uploading file {0} to prefix {1}"
                     .format(source_file, destination_key))
        self.client.create_blob_from_path(self.container_name, destination_key,
                                          source_file)

    def upload_file_obj(self,  destination_key, source_fd):
        self.client.create_blob_from_stream(self.container_name,
                                            destination_key,
                                            source_fd)

    def copy_from_key(self, source_key, destination_key):
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
                                                source_uri)
        # Wait for the copy to be a success
        while copy_properties.status == 'pending':
            # Wait a second before retrying
            time.sleep(1)
            properties = self.client.get_blob_properties(self.container_name,
                                                         destination_key)
            copy_properties = properties.properties.copy
            # TODO(vin): Raise Error if copy_properties errors out

    def delete_key(self, destination_key):
        logger.debug("Deleting key {0}".format(destination_key))
        return self.client.delete_blob(self.container_name, destination_key)
