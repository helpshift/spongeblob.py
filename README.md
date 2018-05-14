# Spongeblob

## Overview
This is a python library for wrapping s3 and wabs blob storage through a common interface.

## Testing
### Local Testing
 The project is configured to be tested with local docker environment by default, which requires docker installed on the system. On MacOS, docker can be installed with `brew cask install docker`, which installs Docker for Mac and `docker-compose` utility required for testing. Local tests then can be performed with `make test` which setups a tox environment with required pytest plugins and fetches required docker images from docker-hub.

### Cloud testing
To test various cloud storages, setup following env variables and set `test_with_docker = False` in `tests/test_spongeblob.py` and run `make test`

- `WABS_ACCOUNT_NAME`
- `WABS_CONTAINER_NAME`
- `WABS_SAS_TOKEN`
- `S3_AWS_KEY`
- `S3_AWS_SECRET`
- `S3_BUCKET_NAME`

## Todo
- [ ] Implement a `download_file_obj` similar to `upload_file_obj` function
- [ ] Configurable `connect_timeout` and `read_timeout` for connections
