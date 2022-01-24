from enum import Enum


class DeployType(Enum):
    DEPLOY = 'deploy'
    LIST = 'list'
    SCALE = 'scale'
    DESTROY = 'destroy'


class ScaleType(Enum):
    UP = 'up'
    DOWN = 'down'


class NodeType(Enum):
    KYLIN = 'kylin'
    SPARK_WORKER = 'spark_worker'


class Cluster(Enum):
    ALL = 'all'
    DEFAULT = 'default'
