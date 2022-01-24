from enum import Enum


class Client(Enum):
    CLOUD_FORMATION = 'cloudformation'
    EC2 = 'ec2'
    RDS = 'rds'
