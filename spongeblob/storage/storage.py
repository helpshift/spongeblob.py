class Storage(object):
    """
    This is the base class for spongeblob. It defines an interface to be
    implemented by various storages
    """

    @classmethod
    def get_retriable_exceptions(cls, method_name=None):
        """This method is to retrieve exceptions that should be retried for the
        method specified

        :returns: A tuple of exceptions
        :rtype: tuple[Exception]

        """
        return ()

    def list_object_keys(self, prefix='', metadata=False, pagesize=1000):
        """List files for the specified prefix. Fetch metdata if set to true

        :param str prefix: String to match when searching files
        :param bool metadata: If set to True, metadata will be fetched, else not.
        :param int pagesize: Limits the number of objects fetched in a single api call
        :returns: A generator of dict describing objects found by api
        :rtype: Iterator[dict]

        The returned dict will look like this
        ``
        {"key": "/key/for/object",
         "size": <size_of_object_in_bytes>,
         "last_modified": <last_modified_timestamp_in_cloud_storage>,
         "metadata": Union(<metadata_dict_of_key>, None),
        }
        ``

        metadata key in dict above will be set to ``None`` if metadata param is
        False, else metadata will be a dict if metadata is set to True, it will
        be empty dict if there is no metadata.

        """
        raise NotImplementedError

    def download_file(self, source_key, destination_file):
        """Download an object to local filesystem

        :param str source_key: Key for object to be downloaded
        :param str destination_file: Path on local filesystem to download file
        :returns: Nothing
        :rtype: None

        """
        raise NotImplementedError

    def upload_file(self, destination_key, source_file, metadata=None):
        """Upload a file from local filesystem

        :param str destination_key: Key where to store object
        :param str source_file: Path on local file system for file to be uploaded
        :param dict metadata: Metadata to be stored along with object
        :returns: Nothing
        :rtype: None

        """
        raise NotImplementedError

    def upload_file_obj(self, destination_key, source_fd, metadata=None):
        """Upload a file from file object

        :param str destination_key: Key where to store object
        :param file source_fd: A file object to be uploaded
        :param dict metadata: Metadata to be stored along with object
        :returns: Nothing
        :rtype: None

        """
        raise NotImplementedError

    def copy_from_key(self, source_key, destination_key, metadata=None):
        """Copy an object from one key to another key on server side

        :param str source_key: Source key for the object to be copied
        :param str destination_key: Destination key to store object
        :param dict metadata: Metadata to be stored along with object
        :returns: Nothing
        :rtype: None

        """
        raise NotImplementedError

    def delete_key(self, destination_key):
        """Delete an object

        :param str destination_key: Destination key for the object to be deleted
        :returns: Nothing
        :rtype: None

        """
        raise NotImplementedError
