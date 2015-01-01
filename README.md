A tiny HTTP service for making requests to S3 and caching objects locally on the filesystem.

Why might this be useful, you ask? 

I spin up clusters which pull data from S3 and have little if any persistent state.  After the cluster is shut down, the
entire filesystem is lost.   At the same time, many jobs on the cluster are using the same reference data taken from S3. 
It's really more efficient to only pull that data from S3 once, and store it on AWS's ephemerial storage.

To run:  python cache.py CONFIG_PATH

Where CONFIG_PATH is a file containing the following settings:

```python
STORAGE_PATH="/tmp/cached_files"
AWS_ACCESS_KEY_ID = '...'
AWS_SECRET_ACCESS_KEY = '...'
```
The service handled a single url request: "http://localhost:5000/get_local?path=..." where the path argument should be of the form "s3://BUCKET/KEYNAME".  The response will contain a local path to the downloaded file where it was stored under the `STORAGE_PATH` config parameter.
