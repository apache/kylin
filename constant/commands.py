from enum import Enum


class Commands(Enum):
    STOP_PROMETHEUS_COMMAND = 'lsof -t -i:9090 | xargs -r kill -9'
    ZKS_CFG_COMMAND = '''echo 'server.1={host1}:2888:3888\nserver.2={host2}:2888:3888\nserver.3={host3}:2888:3888' >> /home/ec2-user/hadoop/zookeeper/conf/zoo.cfg'''
    # because zk need jdk env which created by root and env config in /root/.bash_profile
    ZKS_START_COMMAND = 'sudo su && source ~/.bash_profile && ${ZOOKEEPER_HOME}/bin/zkServer.sh start'
    ZKS_CHECK_CONFIGURED_COMMAND = '''grep -Fq "{host}:2888:3888" /home/ec2-user/hadoop/zookeeper/conf/zoo.cfg; echo $?'''
    ZKS_CHECK_STARTED_COMMAND = 'sudo lsof -i:2181 >/dev/null 2>&1; echo $?'

    START_PROMETHEUS_COMMAND = 'nohup /home/ec2-user/prometheus/prometheus --config.file=/home/ec2-user/prometheus/prometheus.yml >> /home/ec2-user/prometheus/nohup.out 2>&1 &'

    # because SPARK need jdk env which created by root and env config in /root/.bash_profile
    START_SPARK_MASTER_COMMAND = 'sudo su && source ~/.bash_profile && ${SPARK_HOME}/sbin/start-master.sh'

    # Prometheus commands
    PROMETHEUS_CFG_COMMAND = '''echo '  - job_name: "{node}"\n    static_configs:\n      - targets: ["{host}:9100"]' >> /home/ec2-user/prometheus/prometheus.yml '''
    SPARK_DRIVER_METRIC_OF_KYLIN_COMMAND = """echo '  - job_name: "kylin-spark-driver-{node}"\n    metrics_path: /metrics/prometheus\n    static_configs:\n      - targets: ["{host}:4040"]' >> /home/ec2-user/prometheus/prometheus.yml """
    PROMETHEUS_CFG_CHECK_COMMAND = '''grep -Fq "{node}" /home/ec2-user/prometheus/prometheus.yml; echo $?'''
    PROMETHEUS_DELETE_CFG_COMMAND = '''sed -i "/{node}/,+2d" /home/ec2-user/prometheus/prometheus.yml'''
    SPARK_DRIVER_METRIC_OF_KYLIN_DELETE_CFG_COMMAND = '''sed -i "/kylin-spark-driver-{node}/,+2d" /home/ec2-user/prometheus/prometheus.yml'''

    # Spark metric commands
    # Special COMMAND_TEMPLATE for spark metrics into prometheus
    SPARK_MASTER_METRIC_COMMAND = """echo '  - job_name: "{node}"\n    metrics_path: /metrics/master/prometheus\n    static_configs:\n      - targets: ["{host}:8080"]' >> /home/ec2-user/prometheus/prometheus.yml """
    SPARK_APPLICATIONS_METRIC_COMMAND = """echo '  - job_name: "{node}"\n    metrics_path: /metrics/applications/prometheus\n    static_configs:\n      - targets: ["{host}:8080"]' >> /home/ec2-user/prometheus/prometheus.yml """
    SPARK_DRIVER_METRIC_COMMAND = """echo '  - job_name: "{node}"\n    metrics_path: /metrics/prometheus\n    static_configs:\n      - targets: ["{host}:4040"]' >> /home/ec2-user/prometheus/prometheus.yml """
    SPARK_WORKER_METRIC_COMMAND = """echo '  - job_name: "{node}"\n    metrics_path: /metrics/prometheus\n    static_configs:\n      - targets: ["{host}:4041"]' >> /home/ec2-user/prometheus/prometheus.yml """
    SPARK_EXECUTORS_METRIC_COMMAND = """echo '  - job_name: "{node}"\n    metrics_path: /metrics/executors/prometheus\n    static_configs:\n      - targets: ["{host}:4040"]' >> /home/ec2-user/prometheus/prometheus.yml """

    SPARK_DECOMMISION_WORKER_COMMAND = 'sudo su && source ~/.bash_profile && ${SPARK_HOME}/sbin/decommission-worker.sh'
