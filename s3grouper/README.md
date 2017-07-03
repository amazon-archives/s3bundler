# S3Grouper

Takes S3 Inventory manifest file, split and group by either count or size of objects, upload the new manifest files to S3 bucket of choosing.

Input: s3://inventory-bucket/path/to/manifest.json
Output: s3://BUCKET/PREFIX/path-to-manifest-KEY.N.index

### Usage

```
usage: s3grouper.py [-h] -m s3://BUCKET/KEY -b BUCKET [-p PREFIX] [-k KEY]
                    [-s SIZE_THRESHOLD] [-c COUNT_THRESHOLD]
                    [-i INCLUDE_PREFIX] [-no-rr] [-r REGION] [-v] [-d] [-g]

Take inventory of objects, group into separate manifests

optional arguments:
  -h, --help            show this help message and exit
  -m s3://BUCKET/KEY, --manifest s3://BUCKET/KEY
                        Manifest produced by S3 Inventory (s3 path to
                        manifest.json)
  -b BUCKET, --bucket BUCKET
                        Target S3 bucket to write indices
  -p PREFIX, --prefix PREFIX
                        Target S3 prefix
  -k KEY, --key KEY     Target key name for indicies on S3
  -s SIZE_THRESHOLD, --size-threshold SIZE_THRESHOLD
                        Create bundle when over this size (bytes)
  -c COUNT_THRESHOLD, --count-threshold COUNT_THRESHOLD
                        Create bundle when over this number of objects
  -i INCLUDE_PREFIX, --include INCLUDE_PREFIX
                        Only include objects with specified prefix, skip
                        otherwise. (Defaults: include all)
  -no-rr, --no-reduced-redundancy
                        Do not use the default REDUCED_REDUNDANCY Storage
                        Class
  -r REGION, --region REGION
                        Use regional endpoint (Example: us-west-2)
  -v, --verbose         Enable verbose messages
  -d, --debug           Enable debug messages
  -g, --glacier         Glacier mode. Creates manifest only out of Glacier
                        objects.
```

* note `-no-rr` flag is default on. REDUCED REDUNDANCY is not used.

### Defaults

- 65,000 count_threshold
- 4 GiB size_threshold
- PREFIX = s3grouper-output
- KEY = manifest

### Exit codes

```
2 writing to file
3 reading from file
4 uploading
5 downloading
6 compression
7 other AWS API
```

### Example

`$ s3grouper.py -m s3://path/to/inventory/date/manifest.json`
`$ s3grouper.py -m s3://path/to/inventory/date/manifest.json -i just-this-prefix -b output-bucket -p output-prefix -k myKey -c 100000`

