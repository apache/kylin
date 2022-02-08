## Prerequisites 

### Download source code & checkout to a branch of `kylin4_on_cloud`

commands:

```shell
git clone https://github.com/apache/kylin.git && cd kylin && git checkout kylin4_on_cloud
```

### Initiliaze an AWS Account

#### I. Create an `IAM` role<a name="IAM"></a> 

> Note: 
>
> ​	`IAM` role must have the access which contains `AmazonEC2RoleforSSM`, `AmazonSSMFullAccess,` and `AmazonSSMManagedInstanceCore`.
>
> ​	This `IAM` Role will be used to initialize every ec2 instances which is for creating a kylin4 cluster on AWS. And it will configure in `Initialize Env of Local Machine` part.



> Q: Why should need an `IAM` role?
>
> A:  
>
> 	1. At first, the `IAM` role is an IAM identity that you can create in your account that has specific permissions. 
> 	1. An IAM role does not have standard long-term credentials such as a password or access keys associated with it.  Instead, when you assume a role, it provides you with temporary security credentials for your role session. 
> 	1. The access will not cost money. The cost will depend on what you using on S3 and EC2 instances.
> 	1. For more details about `IAM`, please visit the [official website](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html) by aws.



#### II. Create a `User` 

> Note:
>
> ​	The `User` who is created in aws will be used to execute some operations on an EC2 instance. So the `User` must have some authority as below.

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



> Q: What's the relationship between the `User` and `IAM` role?
>
> A: 
>
> 	1. With `IAM` roles, you can establish trust relationships between your ***trusting*** account and other AWS ***trusted*** accounts.
> 	1. A `User` is an account.
> 	1. The establishment of the relationship will cost nothing. 
> 	1. The cost majorly depends on what you are using on S3 and EC2 instances. 



#### III. Login the created `User` to create key pair and Access Key<a name="keypair"></a>

> Note:
>
> ​	To deploy EC2 instances for Kylin4 Cluster need key pair and access key.

##### 1. Create a new `Key Pair` in the `EC2 -> Key pairs`

![key pair](../images/keypair.png)

##### 2. Generate a new `Access Key` in the `My Security Credentials`

> Note: 
>
> ​	Please download the generated CSV file of `Access Key` immediately. Get the `Access Key` and `Secret Key` to initialize local mac to access aws.

![Access Key](../images/accesskey.png)



![Access Key 2](../images/accesskey2.png)

##### (!!!Important)IV. Make sure current account has 32 vCpu at least

The current tool will deploy a cluster by EC2 instances, and `2 cores * 3(for zookeeper) + 2 cores(for monitor services) + 4 cores * 3(for spark slaves) + 4 cores(for spark master) + 4 cores (for kylin)=28  ` cpus to create instances in default.

> Note: **32 vCPUS is the limit of an default aws account.**

**User can check this quota limit in the limit of EC2 dashborad.**

![cpu limit](../images/cpulimit.png)



For more details about user current limit, please check [View your current limits](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-resource-limits.html).

### Initialize a S3 direcotry<a name="S3"></a> 

#### I. Create a directory on S3

> Note: 
>
> ​	This directory will store tars, scripts, and jars below. And it will also be the root path of working dir for kylin4.

Example: make a directory named `kylin4-aws-test`. You can also create a directory named what you like.

![directory](../images/directory.png)



#### **(Optional)** II. Manually Download packages & Upload them to the S3 path which suffix is `*/tar`

> Note:
>
> ​	This step will automatically do by tools. So you can skip this step, or you want to check packages by yourself.



> Create the directory named `tar` **in the path which was created by yourself**.  For example, the full path would be `s3://.../kylin4-aws-test/tar`.
>

1. Download the Kylin4 package on the [official website](https://kylin.apache.org/download/).
2. Download Hadoop, [version 3.2.0](https://archive.apache.org/dist/hadoop/common/hadoop-3.2.0/hadoop-3.2.0.tar.gz).
3. Download Spark with hadoop3.2, [version 3.1.1](https://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz).
4. Download Hive, [version 2.3.9](https://archive.apache.org/dist/hive/hive-2.3.9/apache-hive-2.3.9-bin.tar.gz).
5. Download Zookeeper, [version 3.4.9.](https://archive.apache.org/dist/zookeeper/zookeeper-3.4.9/zookeeper-3.4.9.tar.gz)
6. Download JDK, [version 1.8_301](https://www.oracle.com/java/technologies/javase/javase8u211-later-archive-downloads.html).
7. Download Node Exporter, [version 1.3.1.](https://github.com/prometheus/node_exporter/releases/download/v1.3.1/node_exporter-1.3.1.linux-amd64.tar.gz)
8. Download Prometheus Server, [version 2.31.1](https://github.com/prometheus/prometheus/releases/download/v2.31.1/prometheus-2.31.1.linux-amd64.tar.gz).
9. Download the Kylin4 package with local cache + soft affinity feature by the [public website.](https://s3.cn-north-1.amazonaws.com.cn/public.kyligence.io/kylin/tar/apache-kylin-4.0.0-bin-spark3-soft.tar.gz)



> Note: 
> 	If you want to use Kylin4 with a local cache + soft affinity feature, please download the `experimental` package above.

![tars](../images/tars.png)



#### (Optional) III. Upload  `backup/jars/*` to the S3 Path which suffix is `*/jars`

> Note:
>
> ​	This step will automatically do by tools. So you can skip this step, or you want to check jars by yourself.



> Create the directory named `jars` **in the path which was created by yourself**.  For example, the full path would be `s3://.../kylin4-aws-test/jars`.
>

Kylin4 needed extra jars

- Basic jars
  - commons-configuration-1.3.jar
  - mysql-connector-java-5.1.40.jar
- Local Cache + Soft Affinity feature needed jars
  - alluxio-2.6.1-client.jar
  - kylin-soft-affinity-cache-4.0.0-SNAPSHOT.jar

![jars](../images/jars.png)

#### (Optional) IV. Upload `backup/scripts/*` to the S3 Path which suffix is `*/scripts`

> Note:
>
> ​	This step will automatically do by tools. So you can skip this step, or you want to check scripts by yourself.



> Create the directory named `scripts` **in the path which was created by yourself**.  For example, the full path would be `s3://.../kylin4-aws-test/scripts`.
>

Scripts:

- prepare-ec2-env-for-kylin4.sh
- prepare-ec2-env-for-spark-master.sh
- prepare-ec2-env-for-spark-slave.sh
- prepare-ec2-env-for-static-services.sh
- prepare-ec2-env-for-zk.sh

![scripts](../images/scripts.png)

### Initialize Env Of Local Machine

#### I. Initilize an aws account on local mac to access AWS<a name="localaws"></a>

> Use `Access Key` and `Secret Key` above to Initialize an AWS account on a local mac. 

```shell
$ aws configure
AWS Access Key ID : *************
AWS Secret Access Key : *************
Default region name : cn-northwest-1
Default output format : json
```

> Note:
>
> 1. If this command got the response `-bash: aws: command not found`, please check in  [Getting started with the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html).
> 2. Region name can be in [Available Regions](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html#concepts-available-regions). The default output format is `json`.

#### II. Init a Python virtual env

> Note: 
>
> ​	Make sure that your machine already has a Python which version is 3.6.6 or later.  

commands: 

```shell
$ bin/init.sh
```

> Note: Follow the information after `bin/init.sh` to activate the python virtual env.

#### III. Configure the `kylin_configs.yaml`

**Required parameters**:

- `AWS_REGION`: Current region for EC2 instances.
- `IAMRole`: IAM role which has the access to aws authority. This parameter will be set to the created **name** of the IAM role.
- `S3_URI`: the prefix path of storing `jars/scripts/tar`. For example, this parameter will be set to `s3://.../kylin4-aws-test`.
- `KeyName`: Security key name is a set of security credentials that you use to prove your identity when connecting to an instance. This parameter will be set to the created **name** of key pair`.
- `CIDR_IP`:  An inbound rule permits instances to receive traffic from the specified IPv4 or IPv6 CIDR address range, or the instances associated with the specified security group.
- `DB_IDENTIFIER`: this param should be only one in the `RDS -> Databases`. And it will be the name of created RDS database.
- `DB_PORT`: this param will be the port of created RDS database, default is `3306`.
- `DB_USER`: this param will be a login ID for the master user of your DB instance, the default is `root`.
- `DB_PASSWORD`: this param will be the password of `DB_USER` to access the DB instance. default is `123456test`, it's strongly suggested you change it.

#### IV. Configure the `kylin.properties` in `backup/properties` directories.<a name="cluster"></a>

> About the indexes of clusters:<a name="indexofcluster"></a>
>
> 1. **Tool will deploy a cluster which will mark to `default` if a user does not specify a cluster to deploy.**
> 2. The range of cluster indexes must be in `CLUSTER_INDEXES` which is configured in the `kylin_configs.yml`.
> 3. **`CLUSTER_INDEXES` is for users to create multiple clusters as READ-WRITE separation mode**,  such as a user may want to create 2 query nodes of Kylin and 2 job nodes of Kylin. 
>    1. User can modify the `CLUSTER_INDEXES` to be `(1, 3)`, then as following steps of this module to modify the `kylin.properties` file for clusters.
>    2. User can mark the Kylin node of the `default` cluster to be `query` mode and the Kylin node of the cluster whose index is `1` to be `query` mode.  User can mark the Kylin node of the cluster which index is `2` to be `job` mode and the Kylin node of the cluster which index is `3` to job mode.
>    3. User can also modify the `CLUSTER_INDEXES` to be `(1, 4)`, and mark the Kylin node of clusters whose index is `1` and `2` to be `query` mode and the Kylin node of clusters whose index is `3` and `4` to be `job` mode. Just don't use `default` to mark a cluster and execute exactly deploy cluster commands. For details about commands, please check to [deploy multiple clusters](./Commands.md#deploycluster).
> 4. The list of mark name for clusters will be [`default`, `{cluster num}` ...] and `{cluster num}` is in the range of `CLUSTER_INDEXES`.
> 5. For example, if `CLUSTER_INDEXES` is (1, 3) means that tool can deploy a cluster and it can be marked as `default` or `1` or `2` or `3`.  And tool can execute to deploy total 1(`default`)  + 3(clusters which mark name can be `1`, `2`, `3`) = 4 clusters.
> 6. Every cluster contains 3 `Zookeepers Node`, 1 `Kylin Node`, 1 `Spark Master Node,` and 3 `Spark Slaves Node` after deployed and it can scale needed nodes of Kylin and Spark workers. 
> 7. **The difference between clusters only can be the index or customized configs of EC2 instances or customized properties of Kylin (and spark and zookeeper).**



1. The `kylin.properties` is for starting kylin instance in the cluster. 
2. The default cluster will check the `kylin.properties` in the `backup/properties/default`, and other specific clusters will check the related num directory such as `1`, `2,` and `3`.
3. User needs to create a new dir for the cluster num in `backup/properties`, and name it to the `${cluster num}`, such as `1`, `2` ,`3` and so on. 
4. Following the `2.` step, copy the `kylin.properties.template` which is in `backup/properties/templates` to the related `${cluster num} ` directories， and rename the template to `kylin.properties`. 
5. The range of cluster nums must match the config `CLUSTER_INDEXES`, such as `CLUSTER_INDEXES: (1, 3)` then the directories must be `1`, `2`,`3` in the `backup/properties`.

![kylin properties](../images/kylinproperties.png)

6. Customize the config which you want to change except the config which contains `{{ }}` mark.

7. If the related `kylin.properties` was filled with cluster information, a file named `.rendered` will create in the directory.

![rendered kylin properties](../images/renderedkylinproperties.png)

8. **(Important !!!)** If you want to re-fill the `kylin.properties` for a `kylin` node in a specific cluster,  you need to remove the `.rendered` file and re-copy the `kylin.propertie.template`. Redo steps from `3` to `6`.