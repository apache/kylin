import argparse
import logging.config

from constant.deployment import DeployType, ScaleType, NodeType, Cluster


def deploy_on_aws(deploy_type: str, scale_type: str, node_type: str, cluster: str = None) -> None:
    from engine import Engine
    aws_engine = Engine()
    if not aws_engine.is_ec2_cluster:
        msg = f'Now only supported platform: EC2, please check `DEPLOY_PLATFORM`.'
        raise Exception(msg)

    if deploy_type == DeployType.DEPLOY.value:
        # init env for all clusters
        aws_engine.init_env()

        if cluster == Cluster.ALL.value:
            aws_engine.launch_default_cluster()
            aws_engine.launch_all_clusters()

        if not cluster or cluster == Cluster.DEFAULT.value:
            aws_engine.launch_default_cluster()

        if cluster and cluster.isdigit():
            aws_engine.launch_cluster(cluster_num=int(cluster))

    elif deploy_type == DeployType.DESTROY.value:
        if cluster == Cluster.ALL.value:
            aws_engine.destroy_all_cluster()
            aws_engine.destroy_default_cluster()
            aws_engine.refresh_kylin_properties()
            aws_engine.destroy_rds_and_vpc()

        if not cluster or cluster == Cluster.DEFAULT.value:
            aws_engine.destroy_default_cluster()
            aws_engine.refresh_kylin_properties_in_default()

        if cluster and cluster.isdigit():
            aws_engine.destroy_cluster(cluster_num=int(cluster))
            aws_engine.refresh_kylin_properties_in_clusters(cluster_nums=[int(cluster)])

    elif deploy_type == DeployType.LIST.value:
        aws_engine.list_alive_nodes()
    elif deploy_type == DeployType.SCALE.value:
        if not scale_type or not node_type:
            msg = 'Invalid scale params, `scale-type`, `node-type` must be not None.'
            raise Exception(msg)
        aws_engine.scale_nodes(scale_type, node_type, cluster)


if __name__ == '__main__':
    logging.config.fileConfig('logging.ini')

    parser = argparse.ArgumentParser()
    parser.add_argument("--type", required=False, default=DeployType.LIST.value, dest='type',
                        choices=[e.value for e in DeployType],
                        help="Use 'deploy' to create a cluster or 'destroy' to delete a cluster "
                             "or 'list' to list alive nodes.")
    parser.add_argument("--scale-type", required=False, dest='scale_type',
                        choices=[e.value for e in ScaleType],
                        help="This param must be used with '--type' and '--node-type' "
                             "Use 'up' to scale up nodes or 'down' to scale down nodes. "
                             "Node type will be in ['kylin', 'spark-worker'].")
    parser.add_argument("--node-type", required=False, dest='node_type',
                        choices=[e.value for e in NodeType],
                        help="This param must be used with '--type' and '--scale-type' "
                             "Use 'kylin' to scale up/down kylin nodes "
                             "or 'spark-worker' to scale up/down spark_worker nodes. ")
    # special choices for cluster
    cluster_choices = [str(num) for num in range(1, 6)]
    cluster_choices.append('all')
    cluster_choices.append('default')
    parser.add_argument("--cluster", required=False, dest='cluster', type=str,
                        choices=cluster_choices,
                        help="Use the `num` in the range `CLUSTER_INDEXES` to specify a cluster index "
                             "which can mark what cluster nodes would be deployed or destroyed."
                             "This param must be used with '--type', "
                             "`default` is for to deploy or destroy nodes of `default` cluster.")
    args = parser.parse_args()
    deploy_on_aws(args.type, args.scale_type, args.node_type, args.cluster)
