## Quick Start For Multiple Clusters

> Pre-steps is same as Quick Start steps which is from 1 to 5.

1. Modify the config `CLUSTER_INDEXES` for multiple cluster.

   > Note:
   >
   > 1. `CLUSTER_INDEXES` means that cluster index is in the range of `CLUSTER_INDEXES`. 
   > 2. If user create multiple clusters, `default` cluster always be created. If `CLUSTER_INDEXES` is (1, 3), there will be 4 cluster which contains the cluster 1, 2, 3 and `default` will be created if user execute the commands.
   > 3. Configs for multiple clusters always are same as the `default` cluster to read from `kylin_configs.yaml`

2. Copy `kylin.properties.template` for expecting clusters to deploy, please check the [details](./prerequisites.md#cluster). 

3. Execute commands to deploy `all` clusters.

   ```shell
   python deploy.py --type deploy --cluster all
   ```

4. Destroy all clusters.

   ```shell
   python deploy.py --type destroy --cluster all
   ```
