import os
import socket

import spongeblob as sb
import pytest
import boto3
from azure.storage.blob import BlockBlobService
from azure.common import AzureMissingResourceHttpError

test_with_docker = True

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


def is_open(ip, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((ip, int(port)))
        s.shutdown(2)
        return True
    except (socket.error, socket.timeout):
        return False


if test_with_docker:
    test_creds['s3'] = {'aws_key': 'test',
                        'aws_secret': 'test',
                        'bucket_name': 'test'}
    test_creds['wabs'] = {'account_name': 'devstoreaccount1',
                          'container_name': 'test',
                          'sas_token': 'test'}
else:
    for env_key in test_env_keys:
        provider, key = env_key.split('_', 1)
        try:
            test_creds[provider.lower()][key.lower()] = os.environ[env_key]
        except KeyError:
            raise KeyError('Define Environment Key {0} for testing'
                           .format(env_key))


def teardown_module():
    if not test_with_docker:
        for test_provider in test_providers:
            storage_obj = sb.setup_storage(test_provider,
                                           **test_creds[test_provider])
            try:
                storage_obj.delete_key(test_file1)
                storage_obj.delete_key(test_file2)
            except AzureMissingResourceHttpError:
                pass

@pytest.fixture(scope='session')
def blob_services(docker_ip, docker_services):
    service_ports = {provider: docker_services.port_for(provider, port)
                     for provider, port in (('s3', 8000), ('wabs', 10000))}

    for provider, port in service_ports.items():
        docker_services.wait_until_responsive(
            timeout=30.0, pause=0.1,
            check=lambda: is_open(docker_ip, port))

    urls = {provider: "http://{0}:{1}".format(docker_ip, port)
            for provider, port in service_ports.items()}
    return urls


@pytest.fixture(scope='function')
def upload_file(tmpdir_factory):
    f = tmpdir_factory.mktemp(test_prefix).join('upload_file.txt')
    f.write(test_filecontents)
    return str(f)


@pytest.fixture(scope='function')
def download_file(tmpdir_factory):
    f = tmpdir_factory.mktemp(test_prefix).join('download_file.txt')
    return str(f)


@pytest.fixture(scope='session')
def storage_clients(blob_services):
    clients = {provider: sb.setup_storage(provider, **test_creds[provider])
               for provider in test_providers}

    if test_with_docker:
        if 's3' in test_providers:
            clients['s3'].client = boto3.client('s3',
                                                aws_access_key_id=test_creds['s3']['aws_key'],
                                                aws_secret_access_key=test_creds['s3']['aws_secret'],
                                                endpoint_url=blob_services['s3'])
            clients['s3'].client.create_bucket(Bucket=test_creds['s3']['bucket_name'])
        if 'wabs' in test_providers:
            clients['wabs'].client = BlockBlobService(account_name=test_creds['wabs']['account_name'],
                                                      sas_token=test_creds['wabs']['sas_token'],
                                                      is_emulated=True)
            clients['wabs'].client.create_container(test_creds['wabs']['container_name'])
    return clients


@pytest.mark.parametrize("test_provider", test_providers)
@pytest.mark.xfail(raises=StopIteration)
def test_list_objects_key(test_provider, storage_clients):
    storage_client = storage_clients[test_provider]
    next(storage_client.list_object_keys(test_prefix))


@pytest.mark.parametrize("test_provider", test_providers)
def test_upload_file(test_provider, upload_file, storage_clients):
    storage_client = storage_clients[test_provider]
    storage_client.upload_file(test_file1, upload_file,
                               metadata={"key1": "metadata1"})
    assert next(storage_client.list_object_keys(test_file1))['key'] == test_file1
    assert next(storage_client.list_object_keys(test_file1))['metadata'] is None


@pytest.mark.parametrize("test_provider", test_providers)
def test_uploaded_file_metadata(test_provider, upload_file, storage_clients):
    storage_client = storage_clients[test_provider]
    storage_client.upload_file(test_file1, upload_file,
                               metadata={"key1": "metadata1"})
    obj_data = next(storage_client.list_object_keys(test_file1, metadata=True))
    assert obj_data['metadata']['key1'] == 'metadata1'

@pytest.mark.parametrize("test_provider", test_providers)
def test_copy_from_key(test_provider, storage_clients):
    storage_client = storage_clients[test_provider]
    storage_client.copy_from_key(test_file1, test_file2)
    assert next(storage_client.list_object_keys(test_file2))['key'] == test_file2


# following test passes but doesn't work correctly for paginators with fakes3, but works on s3
@pytest.mark.parametrize("test_provider", test_providers)
def test_pagination(test_provider, storage_clients):
    storage_client = storage_clients[test_provider]
    assert sum(1 for item in storage_client.list_object_keys(test_prefix, pagesize=1)) == 2


@pytest.mark.parametrize("test_provider", test_providers)
@pytest.mark.xfail(raises=StopIteration)
def test_delete_key_test1(test_provider, storage_clients):
    storage_client = storage_clients[test_provider]
    storage_client.delete_key(test_file1)
    next(storage_client.list_object_keys(test_file1))


@pytest.mark.parametrize("test_provider", test_providers)
def test_download_file(test_provider, download_file, storage_clients):
    storage_client = storage_clients[test_provider]
    storage_client.download_file(test_file2, download_file)
    with open(download_file, 'r') as f:
        assert f.read() == test_filecontents


@pytest.mark.parametrize("test_provider", test_providers)
@pytest.mark.xfail(raises=StopIteration)
def test_delete_key_test2(test_provider, storage_clients):
    storage_client = storage_clients[test_provider]
    storage_client.delete_key(test_file2)
    next(storage_client.list_object_keys(test_file2))


@pytest.mark.parametrize("test_provider", test_providers)
@pytest.mark.xfail(raises=StopIteration)
def test_list_objects_keys_again(test_provider, storage_clients):
    storage_client = storage_clients[test_provider]
    next(storage_client.list_object_keys(test_prefix))
