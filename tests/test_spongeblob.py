import spongeblob as sb
import azure
import pytest
import os

test_providers = ('s3', 'wabs')
test_prefix = 'pytest_spongeblob'
test_file1 = 'pytest_spongeblob/test1.txt'
test_file2 = 'pytest_spongeblob/test2.txt'
test_filecontents = 'pytest_spongeblob_contents'
test_creds = {provider: {} for provider in test_providers}
test_env_keys = ['WABS_ACCOUNT_NAME',
                 'WABS_CONTAINER_NAME',
                 'WABS_SAS_TOKEN',
                 'S3_AWS_KEY',
                 'S3_AWS_SECRET',
                 'S3_BUCKET_NAME']

for env_key in test_env_keys:
    provider, key = env_key.split('_', 1)
    try:
        test_creds[provider.lower()][key.lower()] = os.environ[env_key]
    except KeyError:
        raise KeyError('Define Environment Key {0} for testing'
                       .format(env_key))


def teardown_module():
    for test_provider in test_providers:
        storage_obj = sb.setup_storage(test_provider,
                                       **test_creds[test_provider])
        try:
            storage_obj.delete_key(test_file1)
            storage_obj.delete_key(test_file2)
        except azure.common.AzureMissingResourceHttpError:
            pass


@pytest.fixture(scope='function')
def upload_file(tmpdir_factory):
    f = tmpdir_factory.mktemp(test_prefix).join('upload_file.txt')
    f.write(test_filecontents)
    return str(f)


@pytest.fixture(scope='function')
def download_file(tmpdir_factory):
    f = tmpdir_factory.mktemp(test_prefix).join('download_file.txt')
    return str(f)


@pytest.mark.parametrize("test_provider", test_providers)
def test_list_objects_key(test_provider):
    storage_obj = sb.setup_storage(test_provider, **test_creds[test_provider])
    assert storage_obj.list_object_keys(test_prefix) == []


@pytest.mark.parametrize("test_provider", test_providers)
def test_upload_file(test_provider, upload_file):
    storage_obj = sb.setup_storage(test_provider, **test_creds[test_provider])
    storage_obj.upload_file(test_file1, upload_file)
    assert storage_obj.list_object_keys(test_file1)[0]['key'] == test_file1


@pytest.mark.parametrize("test_provider", test_providers)
def test_copy_from_key(test_provider):
    storage_obj = sb.setup_storage(test_provider, **test_creds[test_provider])
    storage_obj.copy_from_key(test_file1, test_file2)
    assert storage_obj.list_object_keys(test_file2)[0]['key'] == test_file2


@pytest.mark.parametrize("test_provider", test_providers)
def test_delete_key(test_provider):
    storage_obj = sb.setup_storage(test_provider, **test_creds[test_provider])
    storage_obj.delete_key(test_file1)
    assert storage_obj.list_object_keys(test_file1) == []


@pytest.mark.parametrize("test_provider", test_providers)
def test_download_file(test_provider, download_file):
    storage_obj = sb.setup_storage(test_provider, **test_creds[test_provider])
    storage_obj.download_file(test_file2, download_file)
    with open(download_file, 'r') as f:
        assert f.read() == test_filecontents
    storage_obj.delete_key(test_file2)
    assert storage_obj.list_object_keys(test_file2) == []


@pytest.mark.parametrize("test_provider", test_providers)
def test_list_objects_keys_again(test_provider):
    storage_obj = sb.setup_storage(test_provider, **test_creds[test_provider])
    assert storage_obj.list_object_keys(test_prefix) == []
