# Amazon S3 Bundler
Amazon S3 Bundler downloads billions of small S3 objects, bundles them into archives, and uploads them back into S3.

s3grouper takes a manifest.json from S3 inventory as input, splits it into manifests by number of objects and total size, and writes them to S3.

s3bundler takes an s3grouper manifest as input either from SQS or CLI argument. It copies the objects into a tar archive or directly to S3 if they are too big. It writes an index with metadata, tags, etc. alongside the archive. If objects were uploaded multipart, then it calculates the md5sum, otherwise it uses ETag. For a variety of S3 errors, objects are written to a DLQ index to be reviewed later.

If SQS is used, a separate thread is used to update the visibility timeout. It will process 2 messages from SQS before killing itself.

s3bundler and s3grouper can be run in ECS. Tasks will be submitted with the manifest.json URI as input for s3grouper. It writes manifests to an S3 bucket with an event that sends newly written objects ending in `.index` to SQS.

s3bundler can run as an ECS service. It can be manually scaled to the number of messages in SQS. 6 containers/VCPU is a reasonable load for small objects. As the average object size increases, network throughput may become a bottleneck. The container instances can be run by spot fleet across a variety of instances with instance storage and reasonable networking. ECS should use the instance storage for the containers. If s3bundler can't handle a manifest, SQS will send it to a DLQ for later review.

# Setting up S3 Storage Inventory
You’ll need to edit the destination inventory bucket and list all of your buckets to be archived in a file.

```
while read bucket; do aws s3api put-bucket-inventory-configuration --bucket $bucket --id export --inventory-configuration '{
        "IncludedObjectVersions": "Current",
        "OptionalFields": [
            "Size",
            "LastModifiedDate",
            "StorageClass"
        ],
        "Schedule": {
            "Frequency": "Weekly"
        },
        "Id": "export",
        "Destination": {
            "S3BucketDestination": {
                "Format": "CSV",
                "Bucket": "arn:aws:s3:::your-inventory-bucket"
            }
        },
        "IsEnabled": true
    }'; done < /tmp/bucketlist
```

# Getting Started

Pre-requisites:
* awscli (running commands)
* boto3 (driver submitting jobs)
* docker (building docker images)

1) Set up two ECR repositories:
```
aws ecr create-repository --repository-name myproj/s3grouper
aws ecr create-repository --repository-name myproj/s3bundler
```

2) Build both docker images using the dockerfile, tag, and push to ECR

```
pushd s3bundler
docker build -f s3bundler.dockerfile -t 's3bundler:latest' .
popd
pushd s3grouper
docker build -f s3grouper.dockerfile -t 's3grouper:latest' .
popd
docker tag s3bundler:latest <ecr_repository:s3bundler>
docker tag s3grouper:latest <ecr_repository:s3grouper>
$(aws ecr get-login)
docker push <ecr_repository:s3grouper>
docker push <ecr_repository:s3bundler>
```

3) Once verified that the images are in ECR, proceed to deploy Cloudformation stack

s3bundler-spotfleet.cfn.py makes different Cloudformation templates depending on the region and instance family specified.

```
pip install troposphere
python s3bundler.cfn.py > s3bundler.cfn.json
python s3bundler-spotfleet.cfn.py -f i3 --region us-east-1 > s3bundler-spotfleet.cfn.json
```

4) First create stack with s3bundler.cfn.json
Note the value of `ECSCluster` in the Outputs, you'll need this as input for the next step.

5) Create stack with s3bundler-spotfleet.cfn.json

You should now have an ECS cluster running on SpotFleet in your environment.
You're now ready to submit jobs using submitmanifest.py!

# Monitoring
The following Cloudwatch metrics are useful for watching the progress.

S3Bundler/Errors - A few of these here and there should not be an issue. If there are a lot, check the logs for throttling or permissions issues. If S3 is throttling you, scale in your ECS cluster. If you see permissions issues, make sure the bucket policy allows the newly created IAM role to access objects. In either case, you will want to manually add the manifest back to the queue.

SQS/ApproximateNumberOfMessagesVisible - Shows the manifests waiting to start processing.
SQS/ApproximateNumberOfMessagesNotVisible - Shows the number of manifests currently being processed.
SQS/NumberOfMessagesDeleted - Shows the manifest completion over time.

EC2Spot/CPUUtilization - It may be useful to show CPU utilization. If it is too high, it may be necessary to tune the s3bundler task to use less.
EC2Spot/PendingCapacity - This will increase if there is contention in the spot market. The overall job may take longer.
EC2Spot/FulfilledCapacity

# Submitting Jobs

`echo s3://inventorybucket/sourcebucket/inventory/2017-03-30T18-06Z/manifest.json | python submitmanifest.py --region us-east-1  -c s3bundler-ECSCluster-YYYYYYYYYYYY -t arn:aws:ecs:us-east-1:xxxxxxxxxxxx:task-definition/s3grouper:7 -- -b s3bundler-archivebucket-yyyyyyyyyyyy -p manifests`

# Common Errors
## Access Denied
If S3Bundler can't read objects in your source buckets, you may need to add the TaskRole created in the s3bundler Cloudformation stack to your whitelists in the bucket policies in the source buckets.

## Throttling
If writes are frequently throttled, you will either need to reduce the concurrency or request that the S3 team prepare the bucket for the throughput needed.
