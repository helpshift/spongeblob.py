import logging

from .storage import Storage
import boto3
from botocore.client import Config
from botocore.exceptions import EndpointConnectionError

logger = logging.getLogger(__name__)


class S3(Storage):
    def __init__(self, aws_key, aws_secret, bucket_name):
        self.bucket_name = bucket_name
        config = Config(connect_timeout=60, read_timeout=60)
        self.extra_args = {'ServerSideEncryption': 'AES256'}
        self.client = boto3.client('s3',
                                   aws_access_key_id=aws_key,
                                   aws_secret_access_key=aws_secret,
                                   config=config)
        logger.debug("Created s3 client object: {0}".format(self.client))

    @classmethod
    def get_retriable_exceptions(cls, method_name=None):
        if method_name.startswith('upload_file'):
            return (EndpointConnectionError,
                    boto3.exceptions.S3UploadFailedError)
        else:
            return (EndpointConnectionError)

    def get_url_prefix(self):
        return '{}/{}/'.format(self.client.meta.endpoint_url, self.bucket_name)

    def list_object_keys(self, prefix=''):
        logger.debug("Listing files for prefix: {0}".format(prefix))
        response = self.client.list_objects(Bucket=self.bucket_name,
                                            Prefix=prefix)
        response = response.get('Contents', [])
        return [{'key': obj['Key'], 'last_modified': obj['LastModified']}
                for obj in response]

    def download_file(self, source_key, destination_file):
        logger.debug("Downloading blob from prefix {0} to file {1}"
                     .format(source_key, destination_file))
        self.client.download_file(self.bucket_name,
                                  source_key,
                                  destination_file)

    def upload_file(self, destination_key, source_file):
        logger.debug("Uploading file {0} to prefix {1}"
                     .format(source_file, destination_key))
        self.client.upload_file(
                        source_file,
                        self.bucket_name,
                        destination_key,
                        ExtraArgs=self.extra_args)

    def upload_file_obj(self, destination_key, source_fd):
        logger.debug("Uploading stream {0} to prefix {1}"
                     .format(source_fd, destination_key))
        self.client.upload_fileobj(
                        source_fd,
                        self.bucket_name,
                        destination_key,
                        ExtraArgs=self.extra_args)

    def copy_from_key(self, source_key, destination_key):
        logger.debug("Copying key {0} -> {1}"
                     .format(source_key, destination_key))
        self.client.copy(CopySource={'Bucket': self.bucket_name,
                                     'Key': source_key},
                         Bucket=self.bucket_name,
                         Key=destination_key,
                         ExtraArgs=self.extra_args)

    def delete_key(self, destination_key):
        logger.debug("Deleting key {0}".format(destination_key))
        return self.client.delete_object(Bucket=self.bucket_name,
                                         Key=destination_key)
