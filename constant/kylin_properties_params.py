from enum import Enum


class KylinProperties(Enum):
    DB_HOST = 'DB_HOST'
    DB_PORT = 'DB_PORT'
    DB_USER = 'DB_USER'
    DB_PASSWORD = 'DB_PASSWORD'

    S3_BUCKET_PATH = 'S3_BUCKET_PATH'
    SPARK_MASTER = 'SPARK_MASTER'
    ZOOKEEPER_HOST = 'ZOOKEEPER_HOST'
