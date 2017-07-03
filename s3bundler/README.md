# S3Bundler
S3Bundler takes a large number of objects and puts them into a small number of larger archives. It downloads all of the objects listed in a manifest, writes them to a tar archive, and uploads it to S3 with an index that includes metadata, tags, and an md5 checksum.

It can be run with a single manifest URI, or it can get them from SQS.

## Usage

```
usage: s3bundler.py [-h] [-q QUEUE] [-m s3://bucket/key] [-b BUCKET]
                    [-p PREFIX] [-f [F [F ...]]] [-s BYTES] [-c] [-v] [-d]

Bundle S3 objects from an inventory into an archive

optional arguments:
  -h, --help            show this help message and exit
  -q QUEUE, --queue QUEUE
                        SQS S3Bundler manifest queue.
  -m s3://bucket/key, --manifest s3://bucket/key
                        Manifest produced by s3grouper
  -b BUCKET, --bucket BUCKET
                        S3 bucket to write archives to
  -p PREFIX, --prefix PREFIX
                        Target S3 prefix
  -f [F [F ...]], --fieldnames [F [F ...]]
                        Field names in order used by s3grouper
  -s BYTES, --maxsize BYTES
                        Objects greater than maxsize will be copied directly
                        to the destination bucket. Metadata will be stored
                        alongside them. Checksums will not be calculated.
                        Default: 2GB
  -c, --compress        Compress archives with gzip
  -v, --verbose         Enable verbose messages
  -d, --debug           Enable debug messages
```

## Optimizations
### S3 Bucket Partitioning
In order to reduce throttling, S3Bundler writes objects using a hashed hex prefix. You may want to open a support case to request that S3 prepare your archive bucket for more PUT requests per second. See http://docs.aws.amazon.com/AmazonS3/latest/dev/request-rate-perf-considerations.html

### Compression
Archives can optionally be compressed to save space on S3 in addition to reducing the number of objects.

### S3 API calls
When dealing with a ton of small objects, S3 API costs can get pretty high. It is not possible to batch GET requests, so the best we can hope for is one call per object. If an object is under 8 MB, it is retrieved with a single request unless there are tags(one call gets all an object's tags). Otherwise, it is a multipart download.

### MD5 Checksums
If an object was uploaded without multipart, then the ETag header is the MD5 checksum of the object. S3bundler only calculates the checksum if the object was originally a multipart upload.

### ECS Service
When run using SQS, it expects to be managed as an ECS Service. After trying to get 100 messages from the queue, it quits. ECS will automatically restart it. If there are any leaks, this should prevent them from causing problems.

## Handling Errors
### Object Errors
If an object can't be downloaded after boto's internal retries, then the key is written to a DLQ index along with the reason for later investigation. The DLQ index is uploaded next to the original manifest when the archive and index are uploaded.

### Other Errors
Other errors cause s3bundler to quit. SQS will make the message available a few more times until it finally sends it to an SQS DLQ for later investigation.

### Logging
When run is ECS, logs are collected using Cloudwatch Logs which can filter and create metrics.

Normally, only job start, finish, and error summaries are logged to save on log ingestion costs.

Filters can look for the strings "ERROR", "begin processing", and "successfully processed".
