# Spongeblob

## Overview
This is a python library for wrapping s3 and wabs blob storage through a common interface.

## Testing
### Cloud testing
To test various cloud storages, setup following env variables and run `make test`

- WABS\_ACCOUNT\_NAME
- WABS\_CONTAINER\_NAME
- WABS\_SAS\_TOKEN
- S3\_AWS\_KEY
- S3\_AWS\_SECRET
- S3\_BUCKET\_NAME
