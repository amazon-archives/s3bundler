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
import troposphere
import troposphere.ec2
import troposphere.s3
import troposphere.cloudformation
import troposphere.sqs
import troposphere.logs
import troposphere.iam
import troposphere.ecs
import ipaddress

t = troposphere.Template()

t.add_version("2010-09-09")

S3ArchiveBucket = t.add_parameter(troposphere.Parameter(
    "S3ArchiveBucket",
    Default="",
    Description="S3 Bucket where you will store the output archives. If empty, it will be created. An existing bucket needs to notify S3BundlerQueueName on new manifests written by s3grouper.",
    Type="String",
))

NeedsArchiveBucket = t.add_condition(
    "NeedsArchiveBucket",
    troposphere.Equals(troposphere.Ref(S3ArchiveBucket), ""))

S3ArchivePrefix = t.add_parameter(troposphere.Parameter(
    "S3ArchivePrefix",
    Default="archive",
    Description="Prefix within S3 Bucket where you will store the output archives",
    Type="String",
))

S3ManifestPrefix = t.add_parameter(troposphere.Parameter(
    "S3ManifestPrefix",
    Default="manifests",
    Description="Prefix in the S3 archive bucket where you will store the intermediate manifests.",
    Type="String",
))

S3InventoryBucket = t.add_parameter(troposphere.Parameter(
    "S3InventoryBucket",
    Description="S3 Bucket where you sent your S3 Storage Inventory. http://docs.aws.amazon.com/AmazonS3/latest/dev/storage-inventory.html",
    Type="String",
))

S3InventoryPrefix = t.add_parameter(troposphere.Parameter(
    "S3InventoryPrefix",
    Default="",
    Description="Prefix within the S3 Bucket where you sent your S3 Storage Inventory. http://docs.aws.amazon.com/AmazonS3/latest/dev/storage-inventory.html",
    Type="String",
))

S3SourceBuckets = t.add_parameter(troposphere.Parameter(
    "S3SourceBuckets",
    Default="*",
    Description="Comma separated list of S3 buckets in ARN format with trailing '/*' with objects that need to be compressed and bundled. Leave '*' for all buckets in the account. e.g. arn:aws:s3:::foo/*,arn:aws:s3:::bar/*,arn:aws:s3:::baz/*",
    Type="CommaDelimitedList"
))

S3GrouperURL = t.add_parameter(troposphere.Parameter(
    "S3GrouperURL",
    Description="Docker URL for s3grouper container",
    Type="String",
))

S3BundlerURL = t.add_parameter(troposphere.Parameter(
    "S3BundlerURL",
    Description="Docker URL for s3bundler container",
    Type="String",
))

S3BundlerQueueARN = t.add_parameter(troposphere.Parameter(
    "S3BundlerQueueARN",
    Default="",
    Description="SQS queue to tell s3bundler what manifests to work on. Leave empty to create a new queue. If it already exists, it needs to allow S3ArchiveBucket to send it messages and to be configured to send messages to S3BundlerDLQ.",
    Type="String",
))

NeedsQueue = t.add_condition(
    "NeedsQueue",
    troposphere.Equals(troposphere.Ref(S3BundlerQueueARN), ""))

S3BundlerDLQARN = t.add_parameter(troposphere.Parameter(
    "S3BundlerDLQARN",
    Default="",
    Description="SQS queue for messages that s3bundler failed to process. Leave empty to create a new queue. If it already exists, it needs to allow S3BundlerQueue to send it messages.",
    Type="String",
))

NeedsDLQ = t.add_condition(
    "NeedsDLQ",
    troposphere.Equals(troposphere.Ref(S3BundlerDLQARN), ""))

MaxSize = t.add_parameter(troposphere.Parameter(
    "MaxSize",
    Type="String",
    Description="Objects greater than maxsize will be copied directly to the destination bucket. Metadata will be stored alongside them. Checksums will not be calculated.",
    Default="2GB",
    AllowedPattern="^\\d+(GB|G|MB|M|KB|K|gb|g|mb|m|kb|k)?$"
))

Compress = t.add_parameter(troposphere.Parameter(
    "Compress",
    Type="String",
    Description="Compress archives with gzip",
    Default="true",
    AllowedValues=["true","false"]
))

CompressTrue = t.add_condition(
    "CompressTrue",
    troposphere.Equals("true", troposphere.Ref(Compress)))

sqsdlq = t.add_resource(troposphere.sqs.Queue(
    "sqsdlq",
    MessageRetentionPeriod=1209600,
    DeletionPolicy=troposphere.Retain,
    Condition="NeedsDLQ"
))

DLQChoice = troposphere.If("NeedsDLQ",
    troposphere.GetAtt(sqsdlq, "Arn"),
    troposphere.Ref(S3BundlerDLQARN))

ManifestQueue = t.add_resource(troposphere.sqs.Queue(
    "ManifestQueue",
    MessageRetentionPeriod=1209600,
    VisibilityTimeout=1800,
    RedrivePolicy=troposphere.sqs.RedrivePolicy(
        deadLetterTargetArn=DLQChoice,
        maxReceiveCount=10
    ),
    DeletionPolicy=troposphere.Retain,
    Condition="NeedsQueue"
))

QueueChoice = troposphere.If("NeedsQueue",
    troposphere.GetAtt(ManifestQueue, "Arn"),
    troposphere.Ref(S3BundlerQueueARN))

ManifestQueuePolicy = t.add_resource(troposphere.sqs.QueuePolicy(
    "ManifestQueuePolicy",
    Queues=[troposphere.Ref(ManifestQueue)],
    PolicyDocument={
      "Version": "2012-10-17",
      "Id": "manifestqueuepolicy",
      "Statement": [
        {
            "Sid": "q1",
            "Effect": "Allow",
            "Principal": {"Service": "s3.amazonaws.com"},
            "Action": "SQS:SendMessage",
            "Resource": troposphere.GetAtt(ManifestQueue,"Arn"),
            "Condition": troposphere.If("NeedsArchiveBucket",
                {"ForAllValues:ArnLike": {
                    "aws:SourceArn": troposphere.Join("",
                        ["arn:aws:s3:::",
                         troposphere.Ref(troposphere.AWS_STACK_NAME),"-archivebucket-*"])
                                }},
                {"ForAllValues:ArnEquals": {
                    "aws:SourceArn": troposphere.Join("", ["arn:aws:s3:::", troposphere.Ref(S3ArchiveBucket)])
                }})
        }
      ]
    },
    DeletionPolicy=troposphere.Retain,
    Condition="NeedsQueue"
))

DLQPolicy = t.add_resource(troposphere.sqs.QueuePolicy(
    "DLQPolicy",
    Queues=[troposphere.Ref(sqsdlq)],
    PolicyDocument={
      "Version": "2012-10-17",
      "Id": "dlqpolicy",
      "Statement": [
        {
          "Sid": "dlq1",
          "Effect": "Allow",
          "Principal": "*",
          "Action": "SQS:SendMessage",
          "Resource": troposphere.GetAtt(sqsdlq,"Arn"),
          "Condition": {
            "ArnEquals": {
              "aws:SourceArn": troposphere.GetAtt(ManifestQueue, "Arn")
            }
          }
        }
      ]
    },
    DeletionPolicy=troposphere.Retain,
    Condition="NeedsDLQ"
))

ArchiveBucket = t.add_resource(troposphere.s3.Bucket(
    "ArchiveBucket",
    Condition="NeedsArchiveBucket",
    DependsOn="ManifestQueuePolicy",
    DeletionPolicy=troposphere.Retain,
    NotificationConfiguration=troposphere.s3.NotificationConfiguration(
        QueueConfigurations=[
            troposphere.s3.QueueConfigurations(
                Filter=troposphere.s3.Filter(
                    S3Key=troposphere.s3.S3Key(
                        Rules=[
                            troposphere.s3.Rules(Name="prefix", Value=troposphere.Ref(S3ManifestPrefix)),
                            troposphere.s3.Rules(Name="suffix", Value=".index")
                        ]
                    )
                ),
                Event="s3:ObjectCreated:Put",
                Queue=QueueChoice
            )
        ]
    ),
    AccessControl="BucketOwnerFullControl",
))

ArchiveS3Choice = troposphere.If("NeedsArchiveBucket",
    troposphere.Ref(ArchiveBucket),
    troposphere.Ref(S3ArchiveBucket))

TaskRole = t.add_resource(troposphere.iam.Role(
    "TaskRole",
    AssumeRolePolicyDocument={
        "Version": "2012-10-17",
        "Statement": [
            {
                "Principal": {
                    "Service": "ecs-tasks.amazonaws.com"
                },
                "Action": "sts:AssumeRole",
                "Sid": "",
                "Effect": "Allow"
            }
        ]
    },
    Policies=[
        troposphere.iam.Policy(PolicyName="s3bundler", PolicyDocument={
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "readonly",
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:GetObjectTagging",
                        "s3:GetObjectVersion"
                    ],
                    "Resource": troposphere.Ref(S3SourceBuckets)
                },
                {
                    "Sid": "inventory",
                    "Effect": "Allow",
                    "Action": [
                        "s3:ListBucket",
                        "s3:GetBucketTagging",
                        "s3:GetBucketLocation",
                        "s3:GetObject",
                        "s3:GetObjectTagging",
                        "s3:GetObjectVersion"
                    ],
                    "Resource": [
                        troposphere.Join("", ["arn:aws:s3:::", troposphere.Join("/", [
                            troposphere.Ref(S3InventoryBucket),
                            troposphere.Ref(S3InventoryPrefix),
                            "*"
                        ])]),
                        troposphere.Join("", ["arn:aws:s3:::", troposphere.Join("/", [
                            troposphere.Ref(S3InventoryBucket),
                        ])])
                    ]
                },
                {
                    "Sid": "archivebucket",
                    "Effect": "Allow",
                    "Action": [
                        "s3:ListBucket",
                        "s3:GetBucketLocation",
                        "s3:GetObject",
                        "s3:PutObject"
                    ],
                    "Resource": [
                        troposphere.Join("", ["arn:aws:s3:::", troposphere.Join("/", [
                            ArchiveS3Choice,
                            troposphere.Ref(S3ArchivePrefix),
                            "*"
                        ])]),
                        troposphere.Join("", ["arn:aws:s3:::", troposphere.Join("/", [
                            ArchiveS3Choice,
                            troposphere.Ref(S3ManifestPrefix),
                            "*"
                        ])]),
                        troposphere.Join("", ["arn:aws:s3:::", troposphere.Join("/", [
                            ArchiveS3Choice
                        ])])
                    ]
                },
                {
                    "Sid": "s3bundlerqueueaccess",
                    "Effect": "Allow",
                    "Action": [
                        "sqs:ChangeMessageVisibility",
                        "sqs:DeleteMessage",
                        "sqs:ReceiveMessage",
                        "sqs:GetQueueUrl",
                        "sqs:GetQueueAttributes"
                    ],
                    "Resource": [ QueueChoice, DLQChoice ]
                }
            ]
        }
        )]
))

s3grouperlogs = t.add_resource(troposphere.logs.LogGroup(
    "s3grouperlogs",
))

s3bundlerlogs = t.add_resource(troposphere.logs.LogGroup(
    "s3bundlerlogs",
))

# LMF1MJK9 = t.add_resource(troposphere.logs.MetricFilter(
#     "LMF1MJK9",
#     LogGroupName=troposphere.Ref(s3grouperlogs),
# ))
#

S3BundlerErrors = t.add_resource(troposphere.logs.MetricFilter(
    "S3BundlerErrors",
    LogGroupName=troposphere.Ref(s3bundlerlogs),
    FilterPattern="\"ERROR:\" -glaciered -deleted",
    MetricTransformations=[troposphere.logs.MetricTransformation(
        MetricNamespace="S3Bundler",
        MetricName="Errors",
        MetricValue="1"
    )]
))

ECSCluster = t.add_resource(troposphere.ecs.Cluster(
    "ECSCluster",
))

s3bundlertask = t.add_resource(troposphere.ecs.TaskDefinition(
    "s3bundlertask",
    TaskRoleArn=troposphere.Ref(TaskRole),
    NetworkMode="host",
    Family="s3bundler",
    Volumes=[troposphere.ecs.Volume(Name="data")],
    ContainerDefinitions=[troposphere.ecs.ContainerDefinition(
        Name="s3bundler",
        Cpu=170,
        MemoryReservation=128,
        Image=troposphere.Ref(S3BundlerURL),
        Environment=[
            troposphere.ecs.Environment(
                Name="AWS_DEFAULT_REGION",
                Value=troposphere.Ref(troposphere.AWS_REGION)),
            troposphere.ecs.Environment(
                Name="TMP",
                Value="/mnt" )
        ],
        Command=[troposphere.If("CompressTrue", "-c", ""),
            "-q",troposphere.Select("5", troposphere.Split(":", QueueChoice)),
            "-b",ArchiveS3Choice,
            "-p",troposphere.Ref(S3ArchivePrefix),
            "-s",troposphere.Ref(MaxSize)],
        DockerLabels= {"Project": "S3Bundler"},
        LogConfiguration=troposphere.ecs.LogConfiguration(
            LogDriver="awslogs",
            Options={
                "awslogs-group": troposphere.Ref(s3bundlerlogs),
                "awslogs-region": troposphere.Ref(troposphere.AWS_REGION)
            }),
        Essential=True,
        DisableNetworking=False,
        ReadonlyRootFilesystem=False,
        MountPoints=[troposphere.ecs.MountPoint(
            SourceVolume="data",
            ContainerPath="/mnt"
        )]
    )]
))

s3groupertask = t.add_resource(troposphere.ecs.TaskDefinition(
    "s3groupertask",
    TaskRoleArn=troposphere.Ref(TaskRole),
    NetworkMode="host",
    Family="s3grouper",
    ContainerDefinitions=[troposphere.ecs.ContainerDefinition(
        Name="s3grouper",
        MemoryReservation=64,
        Image=troposphere.Ref(S3GrouperURL),
        Environment=[troposphere.ecs.Environment(Name="AWS_DEFAULT_REGION",
            Value=troposphere.Ref(troposphere.AWS_REGION))],
        DockerLabels= {"Project": "S3Bundler"},
        LogConfiguration=troposphere.ecs.LogConfiguration(
            LogDriver="awslogs",
            Options={
                "awslogs-group": troposphere.Ref(s3grouperlogs),
                "awslogs-region": troposphere.Ref(troposphere.AWS_REGION)
            }),
        Essential=True,
        DisableNetworking=False,
        ReadonlyRootFilesystem=False,
    )]
))

s3bundlerservice = []
for n in range(0,50):
    s3bundlerservice.append(t.add_resource(troposphere.ecs.Service(
        "s3bundlerservice{0}".format(str(n)),
        Cluster=troposphere.Ref(ECSCluster),
        TaskDefinition=troposphere.Ref(s3bundlertask),
        DesiredCount=0
    )))

t.add_output(troposphere.Output(
    "ECSCluster",
    Value=troposphere.Ref(ECSCluster)
))

t.add_output(troposphere.Output(
    "ArchiveBucket",
    Value=ArchiveS3Choice
))

t.add_output(troposphere.Output(
    "ManifestQueue",
    Value=QueueChoice
))

t.add_output(troposphere.Output(
    "S3GrouperTask",
    Value=troposphere.Ref(s3groupertask)
))

print(t.to_json())
