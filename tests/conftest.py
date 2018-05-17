import os
import socket
import pytest

import boto3
from azure.storage.blob import BlockBlobService


def pytest_addoption(parser):
    parser.addoption("--no-docker", action="store_true", default=False,
                     help=("Don't test with docker. Requires setting "
                           "up storage credential env variables"))
    parser.addoption("--providers", action="store", default="s3,wabs",
                     help="list of providers to test against")


def pytest_generate_tests(metafunc):
    if 'test_provider' in metafunc.fixturenames:
        metafunc.parametrize("test_provider",
                             metafunc.config.getoption("providers").split(","))


@pytest.fixture(scope="session")
def test_with_docker(request):
    return not request.config.getoption("--no-docker")


@pytest.fixture(scope="session")
def test_providers(request):
    return request.config.getoption("--providers").split(",")


@pytest.fixture(scope="session")
def test_data(test_with_docker, test_providers):
    test_data = {
        'providers': test_providers,
        'prefix': 'pytest_spongeblob',
        'file1': 'pytest_spongeblob/test1.txt',
        'file2': 'pytest_spongeblob/test2.txt',
        'filecontents': 'pytest_spongeblob_contents',
        'env_keys': ['WABS_ACCOUNT_NAME',
                     'WABS_CONTAINER_NAME',
                     'WABS_SAS_TOKEN',
                     'S3_AWS_KEY',
                     'S3_AWS_SECRET',
                     'S3_BUCKET_NAME']
    }
    test_creds = {provider: {} for provider in test_data['providers']}
    if test_with_docker:
        test_creds['s3'] = {'aws_key': 'test',
                            'aws_secret': 'test',
                            'bucket_name': 'test'}
        test_creds['wabs'] = {'account_name': 'devstoreaccount1',
                              'container_name': 'test',
                              'sas_token': 'test'}
    else:
        for env_key in test_data['env_keys']:
            provider, key = env_key.split('_', 1)
            if provider.lower() in test_data['providers']:
                try:
                    test_creds[provider.lower()][key.lower()] = os.environ[env_key]
                except KeyError:
                    raise KeyError('Define Environment Key {0} for testing'
                                   .format(env_key))
    test_data['creds'] = test_creds

    return test_data


@pytest.fixture(scope='session')
def docker_compose_file(pytestconfig, test_with_docker):
    tests_dir = os.path.join(str(pytestconfig.rootdir), 'tests')
    if test_with_docker:
        docker_file = "docker-compose.yml"
    else:
        docker_file = "docker-dummy.yml"
    return os.path.join(tests_dir, docker_file)


def is_open(ip, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((ip, int(port)))
        s.shutdown(2)
        return True
    except (socket.error, socket.timeout):
        return False


@pytest.fixture(scope='session')
def blob_services(docker_ip, docker_services, test_with_docker):
    if test_with_docker:
        service_ports = {provider: docker_services.port_for(provider, port)
                         for provider, port in (('s3', 8000), ('wabs', 10000))}

        for provider, port in service_ports.items():
            docker_services.wait_until_responsive(
                timeout=30.0, pause=0.1,
                check=lambda: is_open(docker_ip, port))

        urls = {provider: "http://{0}:{1}".format(docker_ip, port)
                for provider, port in service_ports.items()}
        return urls
    else:
        return {}


@pytest.fixture(scope='function')
def upload_file(tmpdir_factory, test_data):
    test_filecontents = test_data['filecontents']
    test_prefix = test_data['prefix']
    f = tmpdir_factory.mktemp(test_prefix).join('upload_file.txt')
    f.write(test_filecontents)
    return str(f)


@pytest.fixture(scope='function')
def download_file(tmpdir_factory, test_data):
    test_prefix = test_data['prefix']
    f = tmpdir_factory.mktemp(test_prefix).join('download_file.txt')
    return str(f)


@pytest.fixture(scope='session')
def lowlevel_storage_clients(blob_services, test_data, test_with_docker):
    clients = {}
    test_creds = test_data['creds']
    if test_with_docker:
        if 's3' in test_data['providers']:
            clients['s3'] = boto3.client('s3',
                                         aws_access_key_id=test_creds['s3']['aws_key'],
                                         aws_secret_access_key=test_creds['s3']['aws_secret'],
                                         endpoint_url=blob_services['s3'])
            clients['s3'].create_bucket(Bucket=test_creds['s3']['bucket_name'])
        if 'wabs' in test_data['providers']:
            clients['wabs'] = BlockBlobService(account_name=test_creds['wabs']['account_name'],
                                               sas_token=test_creds['wabs']['sas_token'],
                                               is_emulated=True)
            clients['wabs'].create_container(test_creds['wabs']['container_name'])
    return clients
