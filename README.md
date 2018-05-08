# Spongeblob

## Overview
This is a python library for wrapping s3 and wabs blob storage through a common interface.

## Testing
### Local Testing
 The project is configured to be tested with local docker environment by default, which requires docker installed on the system. On MacOS, docker can be installed with `brew cask install docker`, which installs Docker for Mac and `docker-compose` utility required for testing. Local tests then can be performed with `make test` which setups a tox environment with required pytest plugins and fetches required docker images from docker-hub.

### Cloud testing
To test various cloud storages, setup following env variables and set `test_with_docker = False` in `tests/test_spongeblob.py` and run `make test`

- WABS\_ACCOUNT\_NAME
- WABS\_CONTAINER\_NAME
- WABS\_SAS\_TOKEN
- S3\_AWS\_KEY
- S3\_AWS\_SECRET
- S3\_BUCKET\_NAME
