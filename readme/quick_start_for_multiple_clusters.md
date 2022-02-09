## Quick Start For Multiple Clusters

> Pre-steps are the same as Quick Start steps which are from 1 to 5.

1. Modify the config `CLUSTER_INDEXES` for multiple clusters.

   > Note:
   >
   > 1. `CLUSTER_INDEXES` means that cluster index is in the range of `CLUSTER_INDEXES`. 
   > 2. Configs for multiple clusters are also from `kylin_configs.yaml`.
   > 3. For more details about the index of the clusters,  please check [Indexes of clusters](./prerequisites.md#indexofcluster).

2. Copy `kylin.properties.template` for expecting clusters to deploy, please check the [details](./prerequisites.md#cluster). 

3. Execute commands to deploy `all` clusters.

   ```shell
   python deploy.py --type deploy --cluster all
   ```

4. Destroy all clusters.

   ```shell
   python deploy.py --type destroy --cluster all
   ```
