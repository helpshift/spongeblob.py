class Storage(object):

    @classmethod
    def get_retriable_exceptions(cls, method_name=None):
        return ()

    def list_object_keys(self, prefix=''):
        raise NotImplementedError

    def download_file(self, source_key, destination_file):
        raise NotImplementedError

    def upload_file(self, destination_key, source_file, metadata={}):
        raise NotImplementedError

    def upload_file_obj(self, destination_key, source_fd, metadata={}):
        raise NotImplementedError

    def copy_from_key(self, source_key, destination_key, metadata={}):
        raise NotImplementedError

    def delete_key(self, destination_key):
        raise NotImplementedError
