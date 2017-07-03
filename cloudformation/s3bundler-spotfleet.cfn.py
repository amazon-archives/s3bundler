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
import requests
import argparse
import boto3
import os

r = requests.get("http://www.ec2instances.info/instances.json")
pricing = r.json()
instancespecs = dict()
for prod in pricing:
    instancespecs[prod['instance_type']] = prod

parser = argparse.ArgumentParser(description="build a cloudformation template to run a spotfleet cluster.")
parser.add_argument("-f", "--family", nargs='+',
    help="list of EC2 instance families. e.g. c3 or i3")
parser.add_argument("-s", "--cidr",
    help="CIDR block", default="172.21.0.0/16")
parser.add_argument("--region", default=os.environ.get('AWS_DEFAULT_REGION', 'us-east-1'))
args = parser.parse_args()

instancetypes = [x for x in instancespecs.keys() if x.split('.')[0] in args.family]

ec2 = boto3.client('ec2', region_name=args.region)
response = ec2.describe_availability_zones(Filters=[{'Name': 'state', 'Values': ['available'] }])
azs = [az['ZoneName'] for az in response['AvailabilityZones']]

vpccidrblock = ipaddress.ip_network(args.cidr)

t = troposphere.Template()

t.add_version("2010-09-09")

ami = t.add_parameter(troposphere.Parameter(
    "ECSOptimizedAMI",
    Description="EC2 AMI for use with ECS",
    Type="String",
    Default="ami-275ffe31"
))

ECSCluster = t.add_parameter(troposphere.Parameter(
    "ECSClusterName",
    Description="ECS Cluster name for spot fleet instances to join",
    Type="String"
))

SSHKeyPair = t.add_parameter(troposphere.Parameter(
    "SSHKeyPair",
    Description="SSH Key registered in this region",
    Type="AWS::EC2::KeyPair::KeyName",
))

VCPUTarget = t.add_parameter(troposphere.Parameter(
    "VCPUTarget",
    Default="0",
    Description="Number of VPCUs for the ECS Spotfleet cluster",
    Type="Number",
))

VCPUSpotBid = t.add_parameter(troposphere.Parameter(
    "VCPUSpotBid",
    Default="0.00",
    Description="Spot bid per VCPU in the ECS Spotfleet cluster",
    Type="String",
))

VPC = t.add_resource(troposphere.ec2.VPC(
    "VPC",
    CidrBlock=str(vpccidrblock),
))

IGW = t.add_resource(troposphere.ec2.InternetGateway(
    "IGW",
))

AttachGateway = t.add_resource(troposphere.ec2.VPCGatewayAttachment(
    "AttachGateway",
    VpcId=troposphere.Ref(VPC),
    InternetGatewayId=troposphere.Ref(IGW),
))

RouteTable = t.add_resource(troposphere.ec2.RouteTable(
    "RouteTable",
    VpcId=troposphere.Ref("VPC"),
))

Route = t.add_resource(troposphere.ec2.Route(
    "Route",
    DependsOn="AttachGateway",
    RouteTableId=troposphere.Ref(RouteTable),
    GatewayId=troposphere.Ref(IGW),
    DestinationCidrBlock="0.0.0.0/0",
))

subnets = []
for az, cidr in zip(azs, vpccidrblock.subnets(4)):
    subnet = t.add_resource(troposphere.ec2.Subnet(
        "Subnet{}".format(az[-1:]),
        AvailabilityZone=az,
        CidrBlock=str(cidr),
        VpcId=troposphere.Ref("VPC"),
        MapPublicIpOnLaunch=True,
    ))

    subnets.append(subnet)

    RTA = t.add_resource(troposphere.ec2.SubnetRouteTableAssociation(
        "RTA{}".format(az[-1:]),
        RouteTableId=troposphere.Ref(RouteTable),
        SubnetId=troposphere.Ref(subnet),
    ))

spotfleetrole = t.add_resource(troposphere.iam.Role(
    "spotfleetrole",
    AssumeRolePolicyDocument={
        "Statement": [
            {
                "Action": "sts:AssumeRole",
                "Principal": {
                    "Service": "spotfleet.amazonaws.com"
                },
                "Effect": "Allow",
                "Sid": ""
            }
        ],
        "Version": "2012-10-17"
    },
    ManagedPolicyArns=["arn:aws:iam::aws:policy/service-role/AmazonEC2SpotFleetRole"]
))

containerinstancerole = t.add_resource(troposphere.iam.Role(
    "containerinstancerole",
    AssumeRolePolicyDocument={
        "Version": "2012-10-17",
        "Statement": [
            {
                "Principal": {
                    "Service": "ec2.amazonaws.com"
                },
                "Action": "sts:AssumeRole",
                "Sid": "",
                "Effect": "Allow"
            }
        ]
    },
    ManagedPolicyArns=["arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"]
))

containerinstanceprofile = t.add_resource(troposphere.iam.InstanceProfile(
    "containerinstanceprofile",
    Path="/",
    Roles=[troposphere.Ref(containerinstancerole)]
))

s3bundlersg = t.add_resource(troposphere.ec2.SecurityGroup(
    "s3bundlersg",
    VpcId=troposphere.Ref(VPC),
    GroupDescription="s3bundler instances"
))

def mklaunchspecification(InstanceType, WeightedCapacity, VolumeCount):
    spec = {}
    spec["SubnetId"] = troposphere.Join(",", [troposphere.Ref(subnet) for subnet in subnets])
    spec["ImageId"] = troposphere.Ref(ami)
    spec["IamInstanceProfile"] = troposphere.ec2.IamInstanceProfile(Arn=troposphere.GetAtt(containerinstanceprofile, "Arn"))
    spec["WeightedCapacity"] = WeightedCapacity
    spec["SecurityGroups"] = [troposphere.ec2.SecurityGroups(GroupId=troposphere.Ref(s3bundlersg))]
    spec["InstanceType"] = InstanceType
    spec["KeyName"] = troposphere.Ref(SSHKeyPair)
    spec["BlockDeviceMappings"] = [
        troposphere.ec2.BlockDeviceMapping(DeviceName="/dev/xvda",
            Ebs=troposphere.ec2.EBSBlockDevice(
                VolumeType="gp2",
                VolumeSize=8,
                DeleteOnTermination=True)),
        troposphere.ec2.BlockDeviceMapping(DeviceName="/dev/xvdcz", VirtualName="ephemeral99")] #Spotfleet doesn't seem to support NoDevice: {} in cfn
    [spec["BlockDeviceMappings"].append(
        troposphere.ec2.BlockDeviceMapping(
            DeviceName="/dev/xvdc{}".format(chr(ord('a') + i)),
            VirtualName="ephemeral{}".format(str(i))
        )) for i in range(0,VolumeCount)]
    spec["UserData"] = troposphere.Base64(troposphere.Join("", ['''#cloud-boothook

rootvol=`mount | grep " on / " | cut -d\  -f1`
if [ "$rootvol" = "/dev/xvda1" ] || [ "$rootvol" = "/dev/xvde1" ]
then
    prefix="xvd"
else
    prefix="sd"
fi

devices=""
#detect and use instance store volumes
for ephemeral in `curl -s http://169.254.169.254/2016-09-02/meta-data/block-device-mapping/ | grep ephemeral`
do
    device=`curl -s http://169.254.169.254/2016-09-02/meta-data/block-device-mapping/$ephemeral`
    #determine the device path and make sure it matches what the OS uses
    case $device in
        sd*)
            device_path="/dev/`echo $device | sed s/sd/$prefix/`"
        ;;
        xvd*)
            device_path="/dev/`echo $device | sed s/xvd/$prefix/`"
        ;;
    esac

    if [ -b $device_path ]
    then
        devices="$devices $device_path"
    fi
done

#detect and use NVMe devices
for n in /dev/nvme*n1
do
    if [ -b $n ]
    then
        EXT4OPT="-E nodiscard"
        devices="$devices $n"
    fi
done

ephemeral_count=$(IFS=' '; set -- $devices; echo $#)

if [ "$ephemeral_count" -gt 0 ]; then
    for device in $devices; do
        umount $device
        dd if=/dev/zero of=$device bs=512 count=1
        pvcreate -f -y $device
    done

    vgcreate EphemeralVG $devices
    lvcreate -ay -l 100%FREE -i $ephemeral_count EphemeralVG -n Ephemeral
    mkfs.ext4 $EXT4OPT /dev/mapper/EphemeralVG-Ephemeral
    mount -o noatime /dev/mapper/EphemeralVG-Ephemeral /mnt
    echo "/dev/mapper/EphemeralVG-Ephemeral /mnt ext4 noatime 0 0" >> /etc/fstab

    mkdir /mnt/ecs
    chmod 1777 /mnt/ecs
    mkdir /mnt/docker

    echo 'OPTIONS="${OPTIONS} -g /mnt/docker"' >> /etc/sysconfig/docker
    echo 'ECS_DOCKER_GRAPH_PATH=/mnt/docker' >> /etc/ecs/ecs.config

cat << 'EOF' >> /etc/ecs/ecs.config
ECS_CLUSTER=''', troposphere.Ref(ECSCluster), "\n",
'''ECS_DOCKER_GRAPH_PATH=/mnt/docker
EOF
''']))

    return troposphere.ec2.LaunchSpecifications(**spec)

SpotFleet = t.add_resource(troposphere.ec2.SpotFleet(
    "SpotFleet",
    SpotFleetRequestConfigData=troposphere.ec2.SpotFleetRequestConfigData(
        IamFleetRole=troposphere.GetAtt(spotfleetrole, "Arn"),
        SpotPrice=troposphere.Ref(VCPUSpotBid),
        TargetCapacity=troposphere.Ref(VCPUTarget),
        AllocationStrategy="diversified",
        LaunchSpecifications=[mklaunchspecification(i,
                                                    instancespecs[i]['vCPU'],
                                                    instancespecs[i].get('storage', {}).get('devices', 0)) for i in instancetypes])
))

t.add_output(troposphere.Output(
    "SpotFleet",
    Value=troposphere.Ref(SpotFleet)
))

print(t.to_json())
