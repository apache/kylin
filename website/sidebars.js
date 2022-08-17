/**
 * Creating a sidebar enables you to:
 - create an ordered group of docs
 - render a sidebar for each doc of that group
 - provide next/previous navigation

 The sidebars can be generated from the filesystem, or explicitly defined here.

 Create as many sidebars as you want.
 */

// @ts-check

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
    // By default, Docusaurus generates a sidebar from the docs folder structure
    DownloadSideBar: [
        {
            type: 'doc',
            id: 'download',
            label: 'Download',
        },
    ],
    DocumentSideBar: [
        {
            type: 'doc',
            id: 'intro',
            label: 'Introduction to Kylin 5'
        },
        {
            type: 'category',
            label: 'Tutorial',
            items: [
                {
                    type: 'doc',
                    id: 'tutorial/create-a-page'
                },
                {
                    type: 'doc',
                    id: 'tutorial/create-a-document'
                },
                {
                    type: 'doc',
                    id: 'tutorial/create-a-blog-post'
                },
                {
                    type: 'doc',
                    id: 'tutorial/deploy-your-site'
                },
            ]
        },
        {
            type: 'category',
            label: 'Deployment',
            link: {
                type: 'doc',
                id: 'deployment/intro',
            },
            items: [
                {
                    type: 'category',
                    label: 'On Premises',
                    link: {
                        type: 'doc',
                        id: 'deployment/on-premises/intro'
                    },
                    items: [
                        {
                            type: 'doc',
                            id: 'deployment/on-premises/prerequisite',
                        },
                        {
                            type: 'doc',
                            id: 'deployment/on-premises/network_port_requirements',
                        },
                        {
                            type: 'category',
                            label: 'Use RDBMS as Metastore',
                            link: {
                                type: 'doc',
                                id: 'deployment/on-premises/rdbms_metastore/intro',
                            },
                            items: [
                                {
                                    type: 'category',
                                    label: 'MySQL',
                                    link: {
                                        type: 'doc',
                                        id: 'deployment/on-premises/rdbms_metastore/mysql/intro',
                                    },
                                    items: [
                                        {
                                            type: 'doc',
                                            id: 'deployment/on-premises/rdbms_metastore/mysql/install_mysql'
                                        },
                                        {
                                            type: 'doc',
                                            id: 'deployment/on-premises/rdbms_metastore/mysql/mysql_metastore'
                                        },
                                    ],
                                },
                                {
                                    type: 'category',
                                    label: 'PostgreSQL',
                                    link: {
                                        type: 'doc',
                                        id: 'deployment/on-premises/rdbms_metastore/postgresql/intro',
                                    },
                                    items: [
                                        {
                                            type: 'doc',
                                            id: 'deployment/on-premises/rdbms_metastore/postgresql/install_postgresql'
                                        },
                                        {
                                            type: 'doc',
                                            id: 'deployment/on-premises/rdbms_metastore/postgresql/default_metastore'
                                        },
                                    ],
                                },
                            ]
                        },
                        {
                            type: 'category',
                            label: 'Deploy Mode',
                            link: {
                                type: 'doc',
                                id: 'deployment/on-premises/deploy_mode/intro',
                            },
                            items: [
                                {
                                    type: 'doc',
                                    id: 'deployment/on-premises/deploy_mode/cluster_deployment'
                                },
                                {
                                    type: 'doc',
                                    id: 'deployment/on-premises/deploy_mode/service_discovery'
                                },
                                {
                                    type: 'doc',
                                    id: 'deployment/on-premises/deploy_mode/rw_separation'
                                },
                            ]
                        },
                    ],
                },
                {
                    type: 'category',
                    label: 'On Cloud',
                    link: {
                        type: 'doc',
                        id: 'deployment/on-cloud/intro',
                    },
                    items: [
                        {
                            type: 'doc',
                            id: 'deployment/on-cloud/intro'
                        },
                    ]
                },
                // TODO: add Installation part
                // {
                //     type: 'category',
                //     label: 'Installation',
                //     items:[
                //         {
                //
                //         },
                //     ],
                // },
                {
                    type: 'category',
                    label: 'System Configuration',
                    link: {
                        type: 'doc',
                        id: 'configuration/intro',
                    },
                    items: [
                        {
                            type: 'doc',
                            id: 'configuration/configuration'
                        },
                        {
                            type: 'doc',
                            id: 'configuration/spark_dynamic_allocation'
                        },
                        {
                            type: 'doc',
                            id: 'configuration/hadoop_queue_config'
                        },
                        {
                            type: 'doc',
                            id: 'configuration/query_cache'
                        },
                        {
                            type: 'doc',
                            id: 'configuration/https'
                        },
                        {
                            type: 'doc',
                            id: 'configuration/spark_rpc_encryption'
                        },
                        {
                            type: 'doc',
                            id: 'configuration/log_rotate'
                        },
                    ],
                },
                {
                    type: 'doc',
                    id: 'deployment/installation/uninstallation'
                }
            ],
        },
        {
            type: 'category',
            label: 'Operation and Maintenance Guide',
            link: {
                type: 'doc',
                id: 'operations/intro',
            },
            items: [
                {
                    type: 'doc',
                    id: 'operations/overview'
                },
                {
                    type: 'category',
                    label: 'Project Operation',
                    link: {
                        type: 'doc',
                        id: 'operations/project-operation/intro'
                    },
                    items: [
                        {
                            type: 'doc',
                            id: 'operations/project-operation/project_management'
                        },
                        {
                            type: 'doc',
                            id: 'operations/project-operation/project_settings'
                        },
                        {
                            type: 'doc',
                            id: 'operations/project-operation/alerting'
                        },
                    ],
                },
                {
                    type: 'category',
                    label: 'Access Control',
                    link: {
                        type: 'doc',
                        id: 'operations/access-control/intro'
                    },
                    items: [
                        {
                            type: 'doc',
                            id: 'operations/access-control/user_management'
                        },
                        {
                            type: 'doc',
                            id: 'operations/access-control/group_management'
                        },
                        {
                            type: 'category',
                            label: 'Data Access Control',
                            link: {
                                type: 'doc',
                                id: 'operations/access-control/data-access-control/intro'
                            },
                            items: [
                                {
                                    type: 'doc',
                                    id: 'operations/access-control/data-access-control/project_acl'
                                },
                                {
                                    type: 'doc',
                                    id: 'operations/access-control/data-access-control/acl_table'
                                },
                            ],
                        },
                    ],
                },
                {
                    type: 'category',
                    label: 'System Operation',
                    link: {
                        type: 'doc',
                        id: 'operations/system-operation/intro',
                    },
                    items: [
                        {
                            type: 'category',
                            label: 'Diagnosis',
                            link: {
                                type: 'doc',
                                id: 'operations/system-operation/diagnosis/intro',
                            },
                            items: [
                                {
                                    type: 'doc',
                                    id: 'operations/system-operation/diagnosis/diagnosis',
                                },
                                {
                                    type: 'doc',
                                    id: 'operations/system-operation/diagnosis/query_flame_graph',
                                },
                                {
                                    type: 'doc',
                                    id: 'operations/system-operation/diagnosis/build_flame_graph',
                                },
                            ],
                        },
                        {
                            type: 'doc',
                            id: 'operations/system-operation/update-session-table',
                        },
                        {
                            type: 'category',
                            label: 'CLI Operation Tool',
                            link: {
                                type: 'doc',
                                id: 'operations/system-operation/cli_tool/intro',
                            },
                            items: [
                                {
                                    type: 'doc',
                                    id: 'operations/system-operation/cli_tool/environment_dependency_check',
                                },
                                {
                                    type: 'doc',
                                    id: 'operations/system-operation/cli_tool/diagnosis'
                                },
                                {
                                    type: 'category',
                                    label: 'Metadata Tool',
                                    link: {
                                        type: 'doc',
                                        id: 'operations/system-operation/cli_tool/metadata_tool/intro',
                                    },
                                    items: [
                                        {
                                            type: 'doc',
                                            id: 'operations/system-operation/cli_tool/metadata_tool/metadata_backup_restore'
                                        },
                                    ],
                                },
                                {
                                    type: 'doc',
                                    id: 'operations/system-operation/cli_tool/rollback'
                                },
                            ],
                        },
                        {
                            type: 'doc',
                            id: 'operations/system-operation/maintenance_mode'
                        },
                        {
                            type: 'doc',
                            id: 'operations/system-operation/guardian',
                        },
                        {
                            type: 'doc',
                            id: 'operations/system-operation/junk_file_clean',
                        },
                        {
                            type: 'doc',
                            id: 'operations/system-operation/limit_query',
                        },
                    ],
                },
                {
                    type: 'category',
                    label: 'Monitoring',
                    link: {
                        type: 'doc',
                        id: 'operations/monitoring/intro',
                    },
                    items: [
                        {
                            type: 'category',
                            label: 'InfluxDB',
                            link: {
                                type: 'doc',
                                id: 'operations/monitoring/influxdb/intro',
                            },
                            items: [
                                {
                                    type: 'doc',
                                    id: 'operations/monitoring/influxdb/influxdb'
                                },
                                {
                                    type: 'doc',
                                    id: 'operations/monitoring/influxdb/influxdb_maintenance'
                                },
                            ],
                        },
                        {
                            type: 'doc',
                            id: 'operations/monitoring/metrics_intro',
                        },
                        {
                            type: 'doc',
                            id: 'operations/monitoring/service'
                        },
                    ],
                },
                {
                    type: 'category',
                    label: 'Logs',
                    link: {
                        type: 'doc',
                        id: 'operations/logs/intro',
                    },
                    items: [
                        {
                            type: 'doc',
                            id: 'operations/logs/system_log'
                        },
                        {
                            type: 'doc',
                            id: 'operations/logs/audit_log'
                        },
                    ],
                },
            ],
        },
        {
            type: 'category',
            label: 'Modeling',
            items: [
                {
                    type: 'doc',
                    id: 'modeling/intro'
                },
            ],
        },
        {
            type: 'category',
            label: 'Rest API',
            link: {
                type: 'doc',
                id: 'restapi/intro',
            },
            items: [
                {
                    type: 'doc',
                    id: 'restapi/authentication'
                },
                {
                    type: 'doc',
                    id: 'restapi/project_api'
                },
                {
                    type: 'category',
                    label: 'Model API',
                    link: {
                        type: 'doc',
                        id: 'restapi/model_api/intro',
                    },
                    items: [
                        {
                            type: 'doc',
                            id: 'restapi/model_api/model_management_api'
                        },
                        {
                            type: 'doc',
                            id: 'restapi/model_api/model_build_api'
                        },
                        {
                            type: 'doc',
                            id: 'restapi/model_api/model_import_and_export_api'
                        },
                        {
                            type: 'doc',
                            id: 'restapi/model_api/model_multilevel_partitioning_api'
                        },
                    ],
                },
                {
                    type: 'doc',
                    id: 'restapi/segment_management_api'
                },
                {
                    type: 'doc',
                    id: 'restapi/snapshot_management_api'
                },
                {
                    type: 'doc',
                    id: 'restapi/query_api'
                },
                {
                    type: 'doc',
                    id: 'restapi/data_source_api'
                },
                {
                    type: 'doc',
                    id: 'restapi/async_query_api'
                },
                {
                    type: 'doc',
                    id: 'restapi/job_api'
                },
                {
                    type: 'category',
                    label: 'ACL Management API',
                    link: {
                        type: 'doc',
                        id: 'restapi/acl_api/intro',
                    },
                    items: [
                        {
                            type: 'doc',
                            id: 'restapi/acl_api/user_api'
                        },
                        {
                            type: 'doc',
                            id: 'restapi/acl_api/user_group_api'
                        },
                        {
                            type: 'doc',
                            id: 'restapi/acl_api/project_acl_api'
                        },
                        {
                            type: 'doc',
                            id: 'restapi/acl_api/acl_api'
                        },
                    ],
                },
                {
                    type: 'doc',
                    id: 'restapi/streaming_job_api'
                },
                {
                    type: 'doc',
                    id: 'restapi/callback_api'
                },
                {
                    type: 'doc',
                    id: 'restapi/error_code'
                },
            ],
        },
        {
            type: 'category',
            label: 'Query',
            items: [
                {
                    type: 'doc',
                    id: 'query/intro'
                },
            ],
        },

    ],
    DevelopmentSideBar: [
        {
            type: 'category',
            label: 'Development Guide',
            link: {
                type: 'doc',
                id: 'development/intro',
            },
            items: [
                {
                    type: 'doc',
                    id: 'development/roadmap'
                },
                {
                    type: 'doc',
                    id: 'development/how_to_contribute'
                },
                {
                    type: 'doc',
                    id: 'development/how_to_test'
                },
                {
                    type: 'doc',
                    id: 'development/how_to_debug'
                },
                {
                    type: 'doc',
                    id: 'development/how_to_package'
                },
                {
                    type: 'doc',
                    id: 'development/how_to_release'
                },
            ],
        },
    ],
    CommunitySideBar: [
        {
            type: 'doc',
            id: 'community',
            label: 'Community',
        },
    ],
    PowerBySideBar:[
        {
            type: 'doc',
            id: 'powerBy',
            label: 'PowerBy',
        }
    ],
};

module.exports = sidebars;
