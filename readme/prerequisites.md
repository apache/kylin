## Prerequisites

### I. Check user permission

#### 1. Login AWS account and check whether the current user has sufficient permissions in AWS IAM service

![check user permission](../images/check_user_permission.png)

#### 2. Add permission for current user

Users need the following permissions to ensure that subsequent operations can proceed smoothly:

| Services            | Access level                                           | Resources     | Request condition |
| ------------------- | ------------------------------------------------------ | ------------- | ----------------- |
| **Cloudformation**  | **Limited**: List, Read, Write                         | All Resources | None              |
| **EC2**             | **Limited**: Read                                      | All Resources | None              |
| **IAM**             | **Limited**: List, Read, Write, Permissions management | All Resources | None              |
| **RDS**             | **Limited**: List, Write, Tagging                      | All Resources | None              |
| **S3**              | **Limited**: Write, Permissions management, Tagging    | All Resources | None              |
| **SNS**             | **Limited**: List                                      | All Resources | None              |
| **STS**             | **Limited**: Write                                     | All Resources | None              |
| **Systems Manager** | **Limited**: Write                                     | All Resources | None              |

> Note: 
>
> ​	Please add these access levels to separate policy, and if not, there will be a error `Your policy character exceeds the non-whitespace character limit of 6,144.` in the UI.
>
> ​	Or you can also paste the following `json` contents of access levels to one policy. It is already tested OK.



##### How to add the required permissions for user?

###### 1. Create a permission policy in AWS IAM services:

![create permission policy](../images/create_permission_policy.png)

###### 2. Choose `JSON`:

![policy json](../images/policy_json.png)

###### 3. Paste the following content into the `[]` of `Statement`:

```json
{
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "s3:ListAccessPointsForObjectLambda",
                "s3:ListBucket",
                "s3:PutBucketTagging",
                "s3:ListBucketMultipartUploads",
                "s3:ListAllMyBuckets",
                "s3:ListJobs",
                "s3:ListMultipartUploadParts",
                "s3:ListBucketVersions",
                "s3:PutBucketPublicAccessBlock",
                "s3:ListAccessPoints",
                "s3:ListMultiRegionAccessPoints",
                "s3:ListStorageLensConfigurations",
                "ec2:AuthorizeSecurityGroupIngress",
                "ec2:DescribeInstances",
                "ec2:AttachInternetGateway",
                "ec2:CreateRoute",
                "ec2:CreateInternetGateway",
                "ec2:DescribeKeyPairs",
                "ec2:CreateTags",
                "ec2:ModifyNetworkInterfaceAttribute",
                "ec2:RunInstances",
                "ec2:CreateVpcEndpointServiceConfiguration",
                "ec2:CreateNetworkInterface",
                "ec2:CreateVpcEndpointServiceConfiguration",
                "ec2:CreateNetworkInterface",
                "ec2:CreateSubnet",
                "ec2:DescribeSubnets",
                "ec2:CreateNatGateway",
                "ec2:CreateVpc",
                "ec2:DescribeAvailabilityZones",
                "ec2:AttachVpnGateway",
                "ec2:DescribeSecurityGroups",
                "ec2:DescribeVpcs",
                "ec2:AssociateVpcCidrBlock",
                "ec2:AssociateRouteTable",
                "ec2:DescribeInternetGateways",
                "ec2:DescribeAccountAttributes",
                "ec2:DescribeRouteTables",
                "ec2:CreateRouteTable",
                "ec2:AssociateSubnetCidrBlock",
                "ec2:DescribeInstanceTypes",
                "ec2:DescribeVpcEndpoints",
                "ec2:DescribeAddresses",
                "ec2:DescribeInstanceAttribute",
                "ec2:DescribeNetworkInterfaces",
                "ec2:CreateSecurityGroup",
                "ec2:ModifyVpcAttribute",
                "ec2:AuthorizeSecurityGroupEgress",
                "ec2:DescribeNatGateways",
                "ec2:AllocateAddress",
                "ec2:CreateVpcEndpoint",
                "ec2:AttachNetworkInterface",
                "iam:PutRolePolicy",
                "iam:GetRole",
                "iam:GetRolePolicy",
                "iam:CreateInstanceProfile",
                "iam:ListInstanceProfilesForRole",
                "iam:PassRole",
                "iam:ListRoles",
                "iam:CreatePolicy",
                "iam:CreateServiceLinkedRole",
                "iam:CreateRole",
                "iam:GetInstanceProfile",
                "rds:CreateDBSubnetGroup",
                "rds:CreateDBInstance",
                "rds:DescribeDBInstances",
                "rds:AddTagsToResource",
                "rds:DescribeEngineDefaultParameters",
                "rds:CreateDBParameterGroup",
                "rds:DescribeDBSecurityGroups",
                "rds:StartDBInstance",
                "rds:DescribeOrderableDBInstanceOptions",
                "rds:DescribeDBSubnetGroups",
                "rds:DescribeDBParameterGroups",
                "rds:DescribeDBParameters",
                "cloudwatch:GetMetricStatistics",
                "cloudformation:DeleteStack",
                "cloudformation:ValidateTemplate",
                "cloudformation:CreateStackInstances",
                "cloudformation:DescribeStackResources",
                "cloudformation:DescribeStacks",
                "cloudformation:GetTemplate",
                "cloudformation:ListStacks",
                "cloudformation:GetTemplateSummary",
                "cloudformation:CreateStack",
                "cloudformation:DescribeStackEvents",
                "cloudformation:UpdateStack",
                "cloudformation:ListStackResources",
                "cloudformation:GetStackPolicy",
                "cloudformation:DescribeStackResource",
                "cloudformation:DescribeStackInstance",
                "sns:ListTopics",
                "sts:DecodeAuthorizationMessage"
            ],
            "Resource": "*"
        },
        {
            "Sid": "VisualEditor1",
            "Effect": "Allow",
            "Action": "servicequotas:GetServiceQuota",
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "ssm:SendCommand"
            ],
            "Resource": [
                "arn:aws-cn:ssm:*:*:document/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "ssm:SendCommand"
            ],
            "Resource": [
                "arn:aws-cn:ec2:*:*:instance/*"
            ]
        }
```

If there is a problem with the `JSON` here, you can manually add permissions in `Visual editor` according to the contents of the required permission list.

###### 4. Review and create policy

![review policy](../images/review_create_policy.png)

###### 5. Add the permission policy just created to current user:

![add permission](../images/check_user_permission.png)

![add policy to user](../images/add_policy_to_user.png)

### II. Create key pair and Access Key<a name="keypair"></a>

> Note:
>
> ​	To deploy EC2 instances for Kylin4 Cluster need key pair and access key.

#### 1. Create a new `Key Pair` in the `EC2 -> Key pairs`

![key pair](../images/keypair.png)

#### 2. Generate a new `Access Key` in the `My Security Credentials`

> Note:
>
> ​	Please download the generated CSV file of `Access Key` immediately. Get the `Access Key` and `Secret Key` to initialize local machine to access aws.

![Access Key](../images/accesskey.png)

![Access Key 2](../images/accesskey2.png)

### III. Create an `IAM` role<a name="IAM"></a>

Create a role in AWS IAM service to provide temporary access credentials with specific permissions in EC2 nodes created later:

![create_role](../images/create_role.png)

Add permissions policies `AmazonEC2RoleforSSM`, `AmazonSSMFullAccess,` and `AmazonS3FullAccess`, and `AWSGlueConsoleFullAccess` for this role:

![check role permission](../images/check_role_permission.png)

>
> ​	This `IAM` Role will be used to initialize every ec2 instances which is for creating a kylin4 cluster on AWS. And it will configure in `Initialize Env of Local Machine` part.


> Q: Why should need an `IAM` role?
>
> A:  
>
> 	1. At first, the `IAM` role is an IAM identity that you can create in your account that has specific permissions. 
> 	2. An IAM role does not have standard long-term credentials such as a password or access keys associated with it.  Instead, when you assume a role, it provides you with temporary security credentials for your role session.
> 	3. The access will not cost money. The cost will depend on what you using on S3 and EC2 instances.
> 	4. For more details about `IAM`, please visit the [official website](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html) by aws.


> Q: What's the relationship between the `User` and `IAM` role?
>
> A: 
>
> 	1. With `IAM` roles, you can establish trust relationships between your ***trusting*** account and other AWS ***trusted*** accounts.
> 	1. A `User` is an account.
> 	1. The establishment of the relationship will cost nothing. 
> 	1. The cost majorly depends on what you are using on S3 and EC2 instances. 


### (!!!Important)IV. Make sure current account has 32 vCpu at least

The current tool will deploy a cluster by EC2 instances, and `2 cores * 3(for zookeeper) + 2 cores(for monitor services) + 4 cores * 3(for spark slaves) + 4 cores(for spark master) + 4 cores (for kylin)=28  ` cpus to create instances in default.

> Note: **32 vCPUS is the limit of an default aws account.**

**User can check this quota limit in the limit of EC2 dashborad.**

![cpu limit](../images/cpulimit.png)


For more details about user current limit, please check [View your current limits](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-resource-limits.html).

### V. Create a S3 direcotry<a name="S3"></a>

Create a S3 directory as the working directory for deploying kylin cluster:

> Note: 
>
> ​	This directory will store tars, scripts, and jars below. And it will also be the root path of working dir for kylin4.

Example: create a directory named `kylin4-aws-test`. You can also create a directory named what you like.

![directory](../images/directory.png)

During deployment, the required jar packages and other files will be automatically downloaded to this S3 directory, and users do not need to operate by themselves.
If you want to prepare files in S3 directory manually, please check [Prepare files manually](./prepare_files_manually.md)


### VI. Initilize an aws account on local machine to access AWS<a name="localaws"></a>

> Use `Access Key` and `Secret Key` above to Initialize an AWS account on a local machine. 

```shell
$ aws configure
AWS Access Key ID : *************
AWS Secret Access Key : *************
Default region name : ${region_name}
Default output format : json
```

> Note:
>
> 1. If this command got the response `-bash: aws: command not found`, please check in  [Getting started with the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html).
> 2. Region name can be in [Available Regions](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html#concepts-available-regions). The default output format is `json`.
