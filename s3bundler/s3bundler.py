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
import boto3
import botocore
import tarfile
import csv
import tempfile
from io import BytesIO,StringIO,TextIOWrapper
import hashlib
import json
import sys
import atexit
import argparse
from urllib.parse import urlparse, unquote_plus
import os.path
import logging
import threading

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.ERROR)

class Heartbeat(threading.Timer):
    "Run a function on a timer repeatedly until canceled"
    def run(self):
        while not self.finished.is_set():
            logger.info("sending heartbeat")
            self.function(*self.args, **self.kwargs)
            self.finished.wait(self.interval)

class Manifest(object):
    "Downloads and interprets manifests and creates an iterable object for processing S3 objects."
    def __init__(self, bucket, key, message=None, fieldnames=['Bucket', 'Key', 'Size']):
        self.fieldnames = fieldnames
        self.s3 = boto3.client('s3')
        self.message = message
        if message is not None:
            hb = Heartbeat(int(message.Queue().attributes.get('VisibilityTimeout', 30)) - 10,
                message.change_visibility,
                kwargs={'VisibilityTimeout': int(message.Queue().attributes.get('VisibilityTimeout', 30))})
            atexit.register(hb.cancel)
            hb.start()

        self.bucket = bucket
        self.key = key
        if key[-1:].isdigit():
            self.name = os.path.basename(key)
        else:
            self.name = os.path.splitext(os.path.basename(key))[0]
        self.manifestcsv = tempfile.TemporaryFile()
        try:
            self.s3.download_fileobj(self.bucket, self.key, self.manifestcsv)
        except botocore.exceptions.ClientError as e:
            logger.error("ERROR: Failed to download manifest: s3://{}/{}".format(
                self.bucket, self.key))
            logger.debug("Exception: %s", e, exc_info=True)
            sys.exit(5)
        self.manifestcsv.seek(0)
        TextIOmanifestcsv = TextIOWrapper(self.manifestcsv)
        try:
            self.manifestreader = csv.DictReader(TextIOmanifestcsv, fieldnames=fieldnames)
        except csv.Error as e:
            logger.error("ERROR: Failed to open manifest: s3://{}/{}".format(
                self.bucket, self.key))
            logger.debug("Exception: %s", e, exc_info=True)
            sys.exit(3)

        firstline = self.manifestreader.__next__()
        self.sourcebucket = firstline['Bucket']
        TextIOmanifestcsv.seek(0)

        logger.error("begin processing s3://{}/{}".format(self.bucket, self.key))

    def __iter__(self):
        return self

    def __next__(self):
        return self.manifestreader.__next__()

    def __del__(self):
        try:
            hb.cancel()
            logger.debug("cancelling heartbeat")
        except (NameError, AttributeError):
            """not using SQS, so there is no heartbeat thread"""
            pass

    def success(self):
        "Cleanup after successfully processing a manifest"
        try:
            hb.cancel()
        except (NameError, AttributeError):
            """not using SQS, so there is no heartbeat thread"""
            pass

        try:
            self.message.delete()
        except AttributeError:
            """not using SQS, no need to delete message from queue"""
            pass

        logger.error("successfully archived s3://{}/{}".format(self.bucket, self.key))

        self.manifestcsv.close()

class Index(tempfile.SpooledTemporaryFile):
    "Helper to handle creation of indices"
    def __init__(self, name, s3, bucket, key):
        tempfile.SpooledTemporaryFile.__init__(self, max_size=8*1024*1024)
        self.indexname = name
        self.s3 = s3
        self.bucket = bucket
        self.key = key

    def line(self, obj, o):
        if o.get('TagCount', 0) >= 1:
            logger.info("get tags s3://{}/{}".format(obj['Bucket'],obj['Key']))
            tags = self.s3.get_object_tagging(Bucket=obj['Bucket'],
                Key=obj['Key'])['TagSet']
            tagset = ';'.join(("Key={};Value={}".format(t['Key'],t['Value']) for t in tags))
            logger.info("Tags: {}".format(tagset))
        else:
            tagset = ''
        metadata = ';'.join(("{}={}".format(k,v) for k,v in o.get('Metadata', {}).items()))
        if o.get("PartsCount", 1) == 1:
            checksum = o.get('ETag', "")
        else:
            logger.info("calculate checksum s3://{}/{}".format(obj['Bucket'],obj['Key']))
            # calculate md5sum of o['Body']
            # needs to be in separate thread
            try:
                m = hashlib.md5()
                for chunk in iter(lambda: bodyobj.read(4096), b''):
                    m.update(chunk)
                checksum = m.digest()
            except Exception as e:
                logger.error("ERROR: Failed to calculate checksum. continuing. s3://{}/{}".format(
                    obj['Bucket'], obj['Key']))
                logger.debug("Exception: %s", e, exc_info=True)
                checksum=""
        try:
            self.write(','.join((
                obj['Bucket'],
                obj['Key'],
                str(o['ContentLength']),
                str(o['LastModified']),
                str(checksum),
                metadata,
                tagset)).encode() + "\n".encode())
        except Exception as e:
            logger.error("ERROR: Failed to add metadata to index. s3://{}/{}".format(obj['Bucket'], obj['Key']))
            logger.debug("Exception: %s", e, exc_info=True)
            sys.exit(2)

    def push(self):
        self.seek(0)
        try:
            logger.info("uploading {} to s3://{}/{}".format(self.indexname,self.bucket,
                self.key))
            self.s3.upload_fileobj(self, self.bucket, self.key)
        except botocore.exceptions.ClientError as e:
            logger.error("ERROR: Failed to upload index: s3://{}/{}".format(self.bucket,
                self.key))
            logger.debug("Exception: %s", e, exc_info=True)
            sys.exit(4)

class DLQ(Index):
    "The dead letter queue index needs a reason instead of metadata"
    def line(self, obj, reason):
        try:
            self.write(','.join((
                obj['Bucket'],
                obj['Key'],
                obj['Size'],
                obj.get('LastModifiedDate', ''),
                reason)).encode() + "\n".encode())
        except Exception as e:
            logger.error("ERROR: Failed to write to dead letter queue. s3://{}/{}".format(obj['Bucket'], obj['Key']))
            logger.debug("Exception: %s", e, exc_info=True)
            sys.exit(2)

class Archiver(object):
    "Handler to create tar archives of S3 objects in a manifest"
    def __init__(self, bucket, prefix, manifest, maxsize=2*1024*1024, compress=False):
        self.s3 = boto3.client('s3')
        if compress:
            self.tarmode = 'w:gz'
            self.extension = 'tar.gz'
        else:
            self.tarmode='w'
            self.extension = 'tar'

        self.bucket = bucket
        self.prefix = prefix
        self.manifest = manifest
        self.maxsize = maxsize
        self.compress = compress

        self.tarobj = tempfile.TemporaryFile()
        self.target = tarfile.open(mode=self.tarmode, fileobj=self.tarobj)
        hk = hashkey(self.manifest.sourcebucket, self.manifest.name)
        self.indexkey = "{}/{}/{}/{}.index".format(self.prefix.strip('/'),
            hk,
            self.manifest.sourcebucket,
            self.manifest.name)
        self.index = Index("index", self.s3, self.bucket, self.indexkey)
        self.dlq = DLQ("dlq", self.s3, self.manifest.bucket, self.manifest.key + ".dlq")
        self.archivekey = "{}/{}/{}/{}.{}".format(self.prefix.strip('/'),
                                       hk,
                                       self.manifest.sourcebucket,
                                       self.manifest.name,
                                       self.extension)
    def __del__(self):
        logger.info("cleaning up archiver")
        self.tarobj.close()
        self.index.close()
        self.dlq.close()

    def S3ErrorHandler(self, obj, e):
        "Handle S3 exceptions to see if we can recover or need to fail"
        code = e.response["Error"]["Code"]

        logger.error("DEBUG: s3://{}/{} request-id: {} x-amz-id-2: {}".format(obj['Bucket'],
            obj['Key'], e.response['ResponseMetadata']['HTTPHeaders']['x-amz-request-id'],
            e.response['ResponseMetadata']['HTTPHeaders']['x-amz-id-2']))

        if code in ("InternalError","OperationAborted","RequestTimeout","ServiceUnavailable","SlowDown"):
            #retry
            logger.error("ERROR: Skipping {} on {}. Possible throttling. object: s3://{}/{}".format(code,
                e.operation_name, obj['Bucket'], obj['Key']))
            self.dlq.line(obj, "Retry")
        elif str(code) in ("NoSuchKey", "404"):
            #deleted. log and continue
            logger.error("ERROR: Skipping deleted object: s3://{}/{}".format(obj['Bucket'], obj['Key']))
            self.dlq.line(obj, "NoSuchKey")
        elif code == "InvalidObjectState":
            #glacier. log and continue
            logger.error("ERROR: Skipping glaciered object: s3://{}/{}".format(obj['Bucket'], obj['Key']))
            self.dlq.line(obj, "Glacier")
        elif code == "AccessDenied":
            #maybe it is object acl? Log and continue.
            logger.error("ERROR: Skipping AccessDenied object: s3://{}/{}".format(obj['Bucket'], obj['Key']))
            self.dlq.line(obj, "AccessDenied")
        else:
            #Log and quit
            logger.error("ERROR: Failed to {} object: s3://{}/{}".format(
                e.operation_name, obj['Bucket'], obj['Key']))
            logger.error("Exception: %s", e, exc_info=True)
            sys.exit(5)

    def process(self, obj):
        """Download objects and add them to the archive.
        Really big objects are copied without modification."""
        # S3 Storage Inventory encodes to application/x-www-form-urlencoded and converts '%2f' back
        # to '/'
        decodedkey = unquote_plus(obj['Key'])
        if int(obj["Size"]) > self.maxsize:
            """This object is too big. copy to new bucket
               write metadata as key.metadata"""
            try:
                hk = hashkey(self.manifest.sourcebucket, obj['Key'])
                targetkey = '/'.join((self.prefix.strip('/'),
                            hk,
                            self.manifest.sourcebucket,
                            'bigobjects', decodedkey))
                metadatakey = targetkey + '.metadata'
                with Index("metadata", self.s3, self.bucket, metadatakey) as metadata:
                    logger.info("Copying big object s3://{}/{} to s3://{}/{}".format(
                        obj['Bucket'], decodedkey,
                        self.bucket,
                        targetkey))
                    o = self.s3.head_object(Bucket=obj['Bucket'], Key=decodedkey)
                    self.s3.copy({'Bucket': obj['Bucket'], 'Key': decodedkey},
                        self.bucket,
                        targetkey)
                    metadata.line(obj, o)
                    metadata.push()
                return
            except botocore.exceptions.ClientError as e:
                self.S3ErrorHandler(obj, e)
                return
        elif int(obj["Size"]) < 8*1024*1024:
            try:
                logger.info("Downloading to memory s3://{}/{}".format(
                    obj['Bucket'], decodedkey))
                o = self.s3.get_object(Bucket=obj['Bucket'], Key=decodedkey)
            except botocore.exceptions.ClientError as e:
                self.S3ErrorHandler(obj, e)
                return
            bodyobj = o['Body']
        else:
            try:
                logger.info("Downloading to file s3://{}/{}".format(
                    obj['Bucket'], decodedkey))
                o = self.s3.head_object(Bucket=obj['Bucket'], Key=decodedkey)
                bodyobj = tempfile.TemporaryFile()
                self.s3.download_fileobj(obj['Bucket'], decodedkey, bodyobj)
            except botocore.exceptions.ClientError as e:
                self.S3ErrorHandler(obj, e)
                return
            bodyobj.seek(0)
        self.index.line(obj, o)
        info = tarfile.TarInfo(name='/'.join((obj['Bucket'],
                                             decodedkey)))
        info.size = o['ContentLength']
        info.mtime = o['LastModified'].timestamp()
        info.type = tarfile.REGTYPE
        try:
            self.target.addfile(info, bodyobj)
        except Exception as e:
            logger.error("ERROR: Failed to add object to archive. s3://{}/{}".format(obj['Bucket'], decodedkey))
            logger.error("Exception: %s", e, exc_info=True)
            sys.exit(2)

        bodyobj.close()

    def commit(self):
        "Close archive and write index, dlq and tarball to S3"
        self.target.close()
        self.tarobj.seek(0)
        if self.dlq.tell() > 0:
            self.dlq.push()
        if self.index.tell() > 0:
            self.index.push()

            logger.info("uploading archive to s3://{bucket}/{key}".format(
                bucket=self.bucket,key=self.archivekey))
            try:
                self.s3.upload_fileobj(self.tarobj, self.bucket, self.archivekey)
            except botocore.exceptions.ClientError as e:
                logger.error("ERROR: Failed to upload archive: s3://{}/{}".format(self.bucket,
                    self.archivekey))
                logger.error("Exception: %s", e, exc_info=True)
                sys.exit(4)
        self.manifest.success()

def s3uri(uri):
    if not uri.startswith('s3://'):
        raise argparse.ArgumentTypeError("manifest uri is not an s3uri: s3://bucket/key")
    else:
        o = urlparse(uri)
        return {'bucket': o.netloc,
                'key': o.path.lstrip('/')}

def Size(size):
    import re
    sizere = re.compile(r'^(\d+)(gb|g|mb|m|kb|k)?$', re.IGNORECASE)
    m = sizere.match(size)
    if m is None:
        raise argparse.ArgumentTypeError("maxsize must be a number possibly followed by GB|G|MB|M|KB|K")
    elif m.groups()[1] is None:
        return int(m.groups()[0])
    elif m.groups()[1].lower() in ("gb","g"):
        return int(m.groups()[0])*1024*1024*1024
    elif m.groups()[1].lower() in ("mb","m"):
        return int(m.groups()[0])*1024*1024
    elif m.groups()[1].lower() in ("kb","k"):
        return int(m.groups()[0])*1024
    else:
        raise argparse.ArgumentTypeError("Invalid size")

def parsemsg(message):
    "Accepts JSON messages from S3 Events or with Bucket and Key keys"
    try:
        mb = json.loads(message.body)
        if mb.get("Records", None) is not None:
            for event in mb["Records"]:
                if (event.get('eventSource') == 'aws:s3' and
                    event.get('eventName') == 'ObjectCreated:Put'):
                    bucket = event['s3']['bucket']['name']
                    key = event['s3']['object']['key']
                    yield (bucket, key)
        elif (mb.get('Bucket', None) is not None and
              mb.get('Key', None) is not None):
            yield (mb['Bucket'], mb['Key'])
        else:
            raise ValueError
    except json.decoder.JSONDecodeError as e:
        logger.error("ERROR: Invalid message: {}".format(message.body))
        logger.error("ERROR: Invalid JSON in message: %s", e, exc_info=False)
        sys.exit(2)
    except ValueError:
        logger.error("ERROR: Invalid message: {}".format(message.body))
        sys.exit(2)

def hashkey(bucket, key):
    """create a key to use with a prepartitioned bucket:
    https://docs.aws.amazon.com/AmazonS3/latest/dev/request-rate-perf-considerations.html"""
    return hashlib.md5(os.path.join(bucket,key).encode('utf-8')).hexdigest()[0:6]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bundle S3 objects from an inventory into an archive")
    parser.add_argument("-q","--queue", type=str,
                        help="SQS S3Bundler manifest queue.")
    parser.add_argument("-m","--manifest", metavar="s3://bucket/key", type=s3uri,
                        help="Manifest produced by s3grouper")
    parser.add_argument("-b","--bucket", metavar="BUCKET", type=str,
                        help="S3 bucket to write archives to")
    parser.add_argument("-p","--prefix", metavar="PREFIX", type=str,
                        help="Target S3 prefix")
    parser.add_argument("-f","--fieldnames", metavar="F", type=str, nargs="*",
                        help="Field names in order used by s3grouper",
                        default=["Bucket",
                         "Key",
                         "Size"])
    parser.add_argument("-s", "--maxsize", metavar="BYTES", type=Size,
                        default=2*1024*1024*1024,
                        help="Objects greater than maxsize will be copied " +
                        "directly to the destination bucket. Metadata will be" +
                        " stored alongside them. Checksums will not be" +
                        " calculated. Default: 2GB")
    parser.add_argument("-c","--compress", action="store_true", default=False,
                        help="Compress archives with gzip")
    parser.add_argument("-v", "--verbose", action="store_true", default=False,
                        help="Enable verbose messages")
    parser.add_argument("-d", "--debug", action="store_true", default=False,
                        help="Enable debug messages")
    args = parser.parse_args()
    if args.verbose:
        logger.setLevel(logging.INFO)
    if args.debug:
        logger.setLevel(logging.DEBUG)


    if args.queue is not None:
        sqs = boto3.resource('sqs')
        try:
            queue = sqs.get_queue_by_name(QueueName=args.queue)
        except botocore.exceptions.ClientError as e:
            logger.error("ERROR: Failed to open queue {}".format(args.queue))
            logger.debug("Exception: %s", e, exc_info=True)
            sys.exit(2)
        count = 0
        while count < 2:
            try:
                for message in queue.receive_messages(WaitTimeSeconds=20,
                    MaxNumberOfMessages=1):
                    for (bucket, key) in parsemsg(message):
                        logger.info("Received manifest from queue: s3://{}/{}".format(bucket, key))
                        manifest = Manifest(bucket, key, message=message, fieldnames=args.fieldnames)
                        archiver = Archiver(args.bucket, args.prefix, manifest,
                            maxsize=args.maxsize, compress=args.compress)
                        for obj in manifest:
                            archiver.process(obj)
                        archiver.commit()
                        count += 1
            except botocore.exceptions.ClientError as e:
                logger.error("ERROR: Failed to get message from queue {}".format(queue.url))
                logger.debug("Exception: %s", e, exc_info=True)
                sys.exit(2)
    else:
        manifest = Manifest(args.manifest['bucket'], args.manifest['key'], fieldnames=args.fieldnames)
        archiver = Archiver(args.bucket, args.prefix, manifest,
            maxsize=args.maxsize, compress=args.compress)
        for obj in manifest:
            archiver.process(obj)
        archiver.commit()
