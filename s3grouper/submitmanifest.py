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
import sys
import boto3
import argparse
import re
import pprint

tasknamere = re.compile(r'^(arn:aws:ecs:[^:]*:[^:]*:task-definition/)?([^/:]+)(:.*)?$')

parser = argparse.ArgumentParser(description="Submit manifest jobs to s3grouper in ECS",
    usage="%(prog)s [options] -- [s3grouper options]")
parser.add_argument("--region", default=None,
    help="AWS region. Default: value of AWS_DEFAULT_REGION")
parser.add_argument("--profile", default=None,
    help="awscli profile")
parser.add_argument("-f","--file",default="-",
    help="File with list of S3URIs for S3 Storage Inventory manifest.json. Default: stdin")
parser.add_argument("-t","--task", metavar="NAME:TAG",
    help="ECS task name or ARN. If the name is given, the tag is optional.")
parser.add_argument("-c","--cluster",
    help="ECS cluster name")
parser.add_argument("grouperargs", nargs="+")
args = parser.parse_args()

aws = boto3.session.Session(region_name=args.region, profile_name=args.profile)
ecs = aws.client("ecs")

if args.file == "-":
    manifestfile = sys.stdin
else:
    manifestfile = open(args.file, "r")

for manifest in manifestfile.readlines():
    task = ecs.run_task(cluster=args.cluster,
        taskDefinition=args.task,
        overrides={"containerOverrides": [{
            "name": tasknamere.match(args.task).groups()[1],
            "command": args.grouperargs + ["-m", manifest.strip()]}]})
    pprint.pprint(task['tasks'])
manifestfile.close()
