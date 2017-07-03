#!/usr/bin/env python3
##
## Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
##
## Licensed under the Amazon Software License (the "License"). You may not use this
## file except in compliance with the License. A copy of the License is located at
##
##     http://aws.amazon.com/asl/
##
## or in the "license" file accompanying this file. This file is distributed on an
## "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied.
## See the License for the specific language governing permissions and limitations
## under the License.
##
import csv,os,sys,boto3,json,io,argparse
from io import BytesIO, TextIOWrapper
from botocore.exceptions import ClientError
from gzip import GzipFile
from urllib.parse import urlparse
from tempfile import TemporaryFile
import logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.ERROR)

# return headers,list of keys for inventory gz files
def get_gz_from_manifest(s3,manifest):
    try:
        obj = s3.Object(manifest['bucket'], manifest['key'])
        data = BytesIO(obj.get()['Body'].read())
        mjson = json.loads(data.getvalue().decode())
        headers = [x.strip() for x in mjson['fileSchema'].split(',')]
        filelist = [d['key'] for d in mjson['files']]
    except ClientError as e:
        logger.error('Unable to get manifest file at s3://{}/{}'.format(manifest['bucket'], manifest['key']))
        logger.debug('Received error: {}'.format(e))
        sys.exit(5)
    except ValueError as e:
        logger.error('Unable to read manifest file.')
        logger.debug('Received error: {}'.format(e))
        sys.exit(3)
    return headers,filelist

# upload index data to S3
def s3_upload(s3,data,target,out_index_num,norr, extension=".index"):
    keyname = '/'.join([target['prefix'],target['key']+'.'+str(out_index_num)+extension])
    s3uri = 's3://'+target['bucket']+'/'+keyname
    data.seek(0)
    extra_args = {'StorageClass':'REDUCED_REDUNDANCY'}
    if norr:
        extra_args = {}
    try:
        # HeadObject to see if exist (needs ListBucket or this returns 403)
        s3.Object(target['bucket'], keyname).load()
        logger.info("{} exist. Skipping...".format(s3uri))
    except ClientError as ce: # if does not exist, then upload
        if ce.response["Error"]["Code"] == "404":
            try:
                s3.Bucket(target['bucket']).upload_fileobj(data,keyname,ExtraArgs=extra_args)
            except ClientError as e:
                logger.error('Unable to upload to {}'.format(s3uri))
                logger.debug('Received error: {}'.format(e))
                sys.exit(4)
            logger.info("Uploaded to {}".format(s3uri))
        elif ce.response["Error"]["Code"] == "403":
            logger.error('Permission error loading {}\n Please check your IAM policy.'.format(s3uri))
            logger.debug('Received error: {}'.format(ce))
            sys.exit(7)
        else:
            logger.error('Unknown error loading {}'.format(s3uri))
            logger.debug('Received error: {}'.format(ce))
            sys.exit(7)
    return

# helper function to download and yield csv row
def dl_and_parse(s3,headers,keylist,bucket):
    for key in keylist:
        with TemporaryFile() as fp:
            try:
                s3.Bucket(bucket).download_fileobj(key,fp)
            except ClientError as e:
                logger.error('Unable to download s3://{}/{}'.format(bucket, key))
                logger.debug('Received error: {}'.format(e))
                sys.exit(5)
            fp.seek(0)
            with TextIOWrapper(GzipFile(fileobj=fp,mode='r')) as f:
                try:
                    reader = csv.DictReader(f,fieldnames=headers,delimiter=',',quoting=csv.QUOTE_MINIMAL)
                    for row in reader:
                        yield row
                except csv.Error as e:
                    logger.error("Unable to read CSV '{}'".format(reader.line))
                    logger.debug('Received error: {}'.format(e))
                    sys.exit(3)

# main parser
def parse(s3,args,headers,keylist):

    # init
    data = TemporaryFile()
    out_index_num = 0
    curr_size = curr_count = 0
    newkey = args.manifest['key'].split('/')
    newkey.pop() # discard manfiest.json
    newkey+=[args.key]
    target={"bucket":args.bucket,
            "prefix":args.prefix,
            "key":'-'.join(newkey)}
    norr = args.no_reduced_redundancy
    total_count = 0

    glacier_match = False

    for row in dl_and_parse(s3,headers,keylist,args.manifest['bucket']):

        if args.glacier and row['StorageClass'] == 'GLACIER':
            #glacier mode - only objects in glacier matches
            glacier_match = True
        elif not args.glacier and row['StorageClass'] != 'GLACIER':
            #not glacier mode - only objects not glacier matches
            glacier_match = True

        # check key prefix
        if row['Key'].startswith(args.include) and glacier_match:
            glacier_match = False #reset match
            try:
                data.write(','.join([row['Bucket'],row['Key'],row['Size']]).encode())
                data.write('\n'.encode())
            except (TypeError,OSError) as e:
                logger.error("Unable to write {},{},{} to TemporaryFile".format(row['Bucket'],row['Key'],str(row['Size'])))
                logger.debug('Received error: {}'.format(e))
                sys.exit(2)
            curr_size += int(row['Size'])
            curr_count += 1
            total_count += 1

            # upload data upon break conditions
            if curr_size >= args.size_threshold or curr_count >= args.count_threshold:
                logger.info("Uploading {} objects ({} MiB)".format(str(curr_count),str(curr_size/1024/1024)))

                if args.glacier:
                    s3_upload(s3,data,target,out_index_num,norr,".glacier.index")
                else:
                    s3_upload(s3,data,target,out_index_num,norr)

                # reset
                out_index_num+=1
                curr_size = curr_count = 0
                data.close()
                data = TemporaryFile()

    # upload last batch
    logger.info("Uploading {} objects ({} MiB)".format(str(curr_count),str(curr_size/1024/1024)))
    if args.glacier:
        s3_upload(s3,data,target,out_index_num,norr,".glacier.index")
    else:
        s3_upload(s3,data,target,out_index_num,norr)
    data.close()
    logger.info("Total number of objects processed: {}".format(str(total_count)))
    return

def s3uri(uri):
    if not uri.startswith('s3://'):
        raise argparse.ArgumentTypeError("manifest uri is not an s3uri: s3://bucket/key")
    else:
        o = urlparse(uri)
        return {'bucket': o.netloc,
                'key': o.path.lstrip('/'),
                'name': os.path.basename(o.path)}

def main(args):
    session = boto3.session.Session(region_name=args.region)
    s3 = session.resource('s3')
    h,l = get_gz_from_manifest(s3,args.manifest)
    parse(s3,args,h,l)
    sys.exit(0)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Take inventory of objects, group into separate manifests")
    parser.add_argument("-m","--manifest", metavar="s3://BUCKET/KEY", type=s3uri,
                        help="Manifest produced by S3 Inventory (s3 path to manifest.json)",required=True)
    parser.add_argument("-b","--bucket", metavar="BUCKET", type=str,
                        help="Target S3 bucket to write indices",required=True)
    parser.add_argument("-p","--prefix", metavar="PREFIX", type=str,
                        help="Target S3 prefix",default="s3grouper-output")
    parser.add_argument("-k","--key", metavar="KEY", type=str,
                        help="Target key name for indicies on S3",default='manifest')
    parser.add_argument("-s","--size-threshold", metavar="SIZE_THRESHOLD", type=int,
                        help="Create bundle when over this size (bytes)",default=4*1024*1024*1024)
    parser.add_argument("-c","--count-threshold", metavar="COUNT_THRESHOLD", type=int,
                        help="Create bundle when over this number of objects",default=65000)
    parser.add_argument("-i","--include", metavar="INCLUDE_PREFIX", type=str,
                        help="Only include objects with specified prefix, \
                        skip otherwise. (Defaults: include all)", default='')
    parser.add_argument("-no-rr","--no-reduced-redundancy", action="store_true",default="True",
                        help="Do not use the default REDUCED_REDUNDANCY Storage Class")
    parser.add_argument("-r","--region",metavar="REGION",type=str,
                        help="Use regional endpoint (Example: us-west-2)")
    #parser.add_argument("--profile") # not used

    parser.add_argument("-v", "--verbose", action="store_true", default=False,
                        help="Enable verbose messages")
    parser.add_argument("-d", "--debug", action="store_true", default=False,
                        help="Enable debug messages")

    parser.add_argument("-g", "--glacier", action="store_true",
                        help="Glacier mode. Creates manifest only out of Glacier objects.")

    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.INFO)
    if args.debug:
        logger.setLevel(logging.DEBUG)

    main(args)
