## Trouble Shooting

### Cloudformation stack is normal, but services can't access normally.

Q: Sometimes users will found that the stack is normal in the cloudformation dashboard, but services aren't started normally. Such as the port of related service can not access by IP.

A: 

1. Users can login into the instance of related services by the `credential file` and the public `IP` which is in the stack `Outputs`.  Access command will be like `ssh -i xxx.pem  ec2-user@{public ip}` on the terminal.
2. Change to the `root` user by executing `sudo su` on the terminal.
3. Check the `/var/log/cloud-init-output.log` to check any error happened in the processing of deployment.



### The stack of related services is normal, but services aren't started normally.

#### `Kylin` starts failing.

Q: `Kylin` starts failing.

A: 

1. Users login into the `Kylin` instances by the `credential file` and the public `IP` which is in the `Kylin` stack `Outputs`.  Access command will be like `ssh -i xxx.pem  ec2-user@{public ip}` on the terminal.
2. Change to the `root` user by executing `sudo su` on the terminal.
3. Refresh variables of env by executing `source ~/.bash_profile` on the terminal.
4. Get the Kylin home by executing `echo $KYLIN_HOME` on the terminal.
5. Check the logs of Kylin in `$KYLIN_HOME/logs/*`.
6. More details about `Kylin` starting, user can check the scripts in `backup/scripts/prepare-ec2-env-for-kylin4.sh`.



#### `Prometheus` starts failing.

Q: `Prometheus` starts failing.

A:

1. Users login into the `Static Services` instances by the `credential file` and the public `IP` which is in the `Static Services` stack `Outputs`.  Access command will be like `ssh -i xxx.pem  ec2-user@{public ip}` on the terminal.
2. Change to the `root` user by executing `sudo su` on the terminal.
3. Refresh variables of env by executing `source ~/.bash_profile` on the terminal.
4. Get the `Prometheus` home by executing `echo $PROMETHEUS_HOME` on the terminal.
5. Check the logs of `Prometheus` in `$PROMETHEUS_HOME/output.log`.
6. More details about `Prometheus` starting, user can check the scripts in `backup/scripts/prepare-ec2-env-for-static-services.sh`.



#### `Granfana` starts failing.

Q: `Granfana` starts failing.

A:

1. Users login into the `Static Services` instances by the `credential file` and the public `IP` which is in the `Static Services` stack `Outputs`.  Access command will be like `ssh -i xxx.pem  ec2-user@{public ip}` on the terminal.
2. Change to the `root` user by executing `sudo su` on the terminal.
3. `Granfana` is started as a docker service.
   1. Use `docker ps -a` to get the docker id of  `Granfana`.
   2. User `docker exec -it ${granfana docker id}` to into `Granfana` service.
   3. Check the logs of  `Grafana` in `/var/log/grafana/grafana.log`.
4. More details about `Granfana` starting, user can check the scripts in `backup/scripts/prepare-ec2-env-for-static-services.sh`.



#### `Spark` starts failing.

Q: `Spark` starts failing.

A:

1. Users login into the `Spark Master/Spark Slave` instances by the `credential file` and the public `IP` which is in the `Spark Master / Spark Slave` stack `Outputs`.  Access command will be like `ssh -i xxx.pem  ec2-user@{public ip}` on the terminal.
2. Change to the `root` user by executing `sudo su` on the terminal.
3. Refresh variables of env by executing `source ~/.bash_profile` on the terminal.
4. Get the `SPARK` home by executing `echo $SPARK_HOME` on the terminal.
5. Check the logs of `SPARK` in `$SPARK_HOME/logs/*.log`.
6. More details about `Spark` starting, user can check the scripts in `backup/scripts/prepare-ec2-env-for-spark-*.sh`.



#### `Kylin starts` failing because can not connect to the Zookeeper.

Q: `Kylin starts` failing because can not connect to the Zookeeper.

A: 

1. Users login into the `Zookeeper` instances by the `credential file` and the public `IP` which is in the Zookeeper stack `Outputs`.  Access command will be like `ssh -i xxx.pem  ec2-user@{public ip}` on the terminal.
2. Change to the `root` user by executing `sudo su` on the terminal.
3. Refresh variables of env by executing `source ~/.bash_profile` on the terminal.
4. Get the Zookeeper home by executing `echo $ZOOKEEPER_HOME` on the terminal.
5. Check the Zookeeper config file in `$ZOOKEEPER_HOME/conf/zoo.cfg`.
6. Check the logs of Zookeeper in `/tmp/zookeeper/zk{1..3}/log`.
7. Check the data-dir of Zookeeper in ``/tmp/zookeeper/zk{1..3}/data`.
8. More details about `Zookeeper` starting, user can check the scripts in `backup/scripts/prepare-ec2-env-for-zk.sh`.



# TO BE CONTINUED...



