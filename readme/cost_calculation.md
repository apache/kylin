## Cost Calculation

### Cost about a cluster

Full cost about a cluster by tool contains as follows:

1. The type of EC2 instances.
2. The volume of EBS for EC2 instances.
3. The volume of S3 is in use.
4. The data transfer for EC2 instances.
5. The RDS.

#### The type of EC2 instances

A total of EC2 instances contains 3 `m5.large` (for zookeeper),  1 `m5.large`(for monitor services), 3 `m5.xlarge`(for spark slaves), 1 `m5.xlarge`(for spark master) and 1 `m5.xlarge` (for kylin).

For example, if the current region is `ap-northeast-2` which is `Asia Pacific (Seoul)`, then user may cost **(3 + 1) * 0.118 + (3 + 1 + 1) * 0.236 = 1.652 USD/hourly**.

> Note: 
>
> 1. The type of EC2 instances for related services can be checked in the `cloudformation_templates/*.yaml`.
> 2. Other scaled up/down nodes can also calculate in manually if users scaled.
> 3. For more details about the cost of EC2 instances, please check [On-Demand Plans for Amazon EC2](https://aws.amazon.com/ec2/pricing/on-demand/).



#### The volume of EBS for EC2 instances

The default volume type for tool is `gp2`.

A total volume size of a cluster contains 10 * 3 (for zookeeper),  20 * 1 (for monitor services), 30 * 3 (for spark slaves), 30 * 1 (for spark master) and 30 * 1(for kylin).

For example, if the current region is `ap-northeast-2` which is `Asia Pacific (Seoul)`, then user may cost **[(10 * 3) + 20 * 1 + 30 * 3 + 30 * 1 + 30 * 1] *  0.114 = 22.8 USD/Per month which equals to 22.8 / (30d * 24h) = 0.0317 USD/hourly.**



> Note:
>
> 1. The volume of EBS for EC2 instances for related services can be checked in the `cloudformation_templates/*.yaml`.
> 2. Other scaled up/down nodes can also calculate manually if users scaled.
> 3. For more details about the cost volume of EBS, please check [Amazon EBS pricing](https://aws.amazon.com/ebs/pricing/).



#### The volume of S3 is in use

the S3 volume pricing is a tiny fraction of the overall cost. **Data on S3 can be stored for the long term.**

For example, if the current region is `ap-northeast-2` which is `Asia Pacific (Seoul)`, and a built data by Kylin such as `kylin_sales` and other needed files in the cluster will take about `2` GB size on the S3. So user may cost **2 * 0.025 GB = 0.05 USD.** **Note that there is no time limit.**

> Note: For more details about the cost volume of S3, please check [Amazon S3 pricing](https://aws.amazon.com/s3/pricing/).

#### The data transfer for EC2 instances

**The tool will upload needed files for clusters to S3, and it's free.**

**And Data transferred between Amazon EC2, Amazon RDS, Amazon Redshift, Amazon ElastiCache instances, and Elastic Network Interfaces in the same Availability Zone is free.**

It will take a minimal fee for a user to login into EC2 instances by public IP. And it's hard to calculate. For more details please check [Data Transfer within the same AWS Region](https://aws.amazon.com/ec2/pricing/on-demand/#Data Transfer within the same AWS Region).

#### The RDS

The type of current RDS is `db.t3.micro`.

The volume size of current RDS is `10` GB.

For example, if the current region is `ap-northeast-2` which is `Asia Pacific (Seoul)`, and a user may cost **0.026 USD** for the type of RDS and 0.131 per GB-month which equals to 0.131/(30d * 24h) = **0.00018 USD/hourly**.

**So it will total cost 0.026 + 0.00018 = 0.02618 USD/hourly.**



### Total Cost

**As discuss above, the total cost is about 1.652 + 0.0317 + 0.05 + 0.02618 = 1.75988 USD/hourly.**

