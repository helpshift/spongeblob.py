import logging

from .storage import Storage
import boto3
from botocore.client import Config
from botocore.exceptions import EndpointConnectionError

logger = logging.getLogger(__name__)


class S3(Storage):
    """
    A class for managing objects on AWS S3. It implements the interface of
    Storage base class
    """

    def __init__(self, aws_key, aws_secret, bucket_name):
        """Setup a S3 storage client object

        :param aws_key: AWS key for the S3 bucket
        :param aws_secret: AWS secret for the S3 bucket
        :param bucket_name: AWS S3 bucket name to connect to

        """
        self.bucket_name = bucket_name
        config = Config(connect_timeout=60, read_timeout=60)
        self.default_extra_args = {'ServerSideEncryption': 'AES256'}
        self.client = boto3.client('s3',
                                   aws_access_key_id=aws_key,
                                   aws_secret_access_key=aws_secret,
                                   config=config)
        logger.debug("Created s3 client object: {0}".format(self.client))

    @classmethod
    def get_retriable_exceptions(cls, method_name=None):
        """Return exceptions that should be retried for specified method of class

        :param str method_name: A method of class for which retriable exceptions should be searched
        :returns: A tuple of exception class to be retried
        :rtype: tuple

        """
        if method_name.startswith('upload_file'):
            return (EndpointConnectionError,
                    boto3.exceptions.S3UploadFailedError)
        else:
            return (EndpointConnectionError)

    def make_extra_args(self, metadata={}):
        """An internal utility function to generate extra args for Boto S3
        uploads. This copies the default `extra_args` class variable and adds
        any the metadata param if not empty.

        :param dict metadata: Metadata to be passed along with extra_args
        :returns: A dictionary of extra args for boto S3 client uploads
        :rtype: dict

        """
        extra_args = self.default_extra_args.copy()
        if metadata:
            extra_args.update({"Metadata": metadata})
        return extra_args

    def get_url_prefix(self):
        """Returns a connection string for the client object

        :returns: Connection string for the client object
        :rtype: str

        """
        return '{}/{}/'.format(self.client.meta.endpoint_url, self.bucket_name)

    def list_object_keys(self, prefix='', metadata=False, pagesize=1000):
        """List object keys matching a prefix for the S3 client

        :param str prefix: A prefix string to list objects
        :param bool metadata: If set to True, object metadata will be fetched with object. Default is False
        :param int pagesize: Maximum objects to be fetched in a single S3 api call. This is limited to upto 1000 objects in S3
        :returns: A generator of object dictionary with key, size and last_modified keys. Metadata will be fetched if set to True
        :rtype: Iterator[dict]

        """
        logger.debug("Listing files for prefix: {0}".format(prefix))

        paginator = self.client.get_paginator('list_objects')
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix,
                                       PaginationConfig={'PageSize': pagesize}):
            if page['Marker']:
                logger.debug("Paging objects "
                             "from marker '{0}'".format(page['Marker']))
            for obj in page.get('Contents', []):
                obj_metadata = None
                if metadata:
                    obj_metadata = self.client.head_object(
                        Bucket=self.bucket_name,
                        Key=obj['Key'])['Metadata']

                yield {'key': obj['Key'],
                       'last_modified': obj['LastModified'],
                       'size': obj['Size'],
                       'metadata': obj_metadata}

    def download_file(self, source_key, destination_file):
        """Download an object from S3 bucket to local filesystem

        :param str source_key: Key for object to be downloaded
        :param str destination_file: Path on local filesystem to download file
        :returns: Nothing
        :rtype: None

        """
        logger.debug("Downloading blob from prefix {0} to file {1}"
                     .format(source_key, destination_file))
        self.client.download_file(self.bucket_name,
                                  source_key,
                                  destination_file)

    def upload_file(self, destination_key, source_file, metadata={}):
        """Upload a file from local filesystem to S3

        :param str destination_key: Key where to store object
        :param str source_file: Path on local file system for file to be uploaded
        :param dict metadata: Metadata to be stored along with object
        :returns: Nothing
        :rtype: None

        """
        logger.debug("Uploading file {0} to prefix {1}"
                     .format(source_file, destination_key))

        self.client.upload_file(
                        source_file,
                        self.bucket_name,
                        destination_key,
                        ExtraArgs=self.make_extra_args(metadata))

    def upload_file_obj(self, destination_key, source_fd, metadata={}):
        """Upload a file from file object to S3

        :param str destination_key: Key where to store object
        :param file source_fd: A file object to be uploaded
        :param dict metadata: Metadata to be stored along with object
        :returns: Nothing
        :rtype: None

        """
        logger.debug("Uploading stream {0} to prefix {1}"
                     .format(source_fd, destination_key))

        self.client.upload_fileobj(
                        source_fd,
                        self.bucket_name,
                        destination_key,
                        ExtraArgs=self.make_extra_args(metadata))

    def copy_from_key(self, source_key, destination_key, metadata={}):
        """Copy a S3 object from one key to another key on server side

        :param str source_key: Source key for the object to be copied
        :param str destination_key: Destination key to store object
        :param dict metadata: Metadata to be stored along with object
        :returns: Nothing
        :rtype: None

        """
        logger.debug("Copying key {0} -> {1}"
                     .format(source_key, destination_key))

        self.client.copy(CopySource={'Bucket': self.bucket_name,
                                     'Key': source_key},
                         Bucket=self.bucket_name,
                         Key=destination_key,
                         ExtraArgs=self.make_extra_args(metadata))

    def delete_key(self, destination_key):
        """Delete an object from S3

        :param str destination_key: Destination key for the object to be deleted
        :returns: Returns a dict, with information about delete operation. Refer: boto3 api for details
        :rtype: dict

        """
        logger.debug("Deleting key {0}".format(destination_key))
        return self.client.delete_object(Bucket=self.bucket_name,
                                         Key=destination_key)
