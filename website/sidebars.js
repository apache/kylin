/**
 * Creating a sidebar enables you to:
 - create an ordered group of docs
 - render a sidebar for each doc of that group
 - provide next/previous navigation

 The sidebars can be generated from the filesystem, or explicitly defined here.

 Create as many sidebars as you want.
 */

// @ts-check

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
            label: 'Quick Start',
            link: {
                type: 'doc',
                id: 'quickstart/intro',
            },
            items: [
                {
                    type: 'doc',
                    id: 'quickstart/quick_start',
                },
                {
                    type: 'doc',
                    id: 'quickstart/expert_mode_tutorial',
                },
                {
                    type: 'doc',
                    id: 'quickstart/sample_dataset',
                },
            ],
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
                    type: 'category',
                    label: 'Install and Uninstall',
                    link: {
                        type: 'doc',
                        id: 'deployment/installation/intro',
                    },
                    items: [
                        {
                            type: 'category',
                            label: 'Install On Platforms',
                            link: {
                                type: 'doc',
                                id: 'deployment/installation/platform/intro',
                            },
                            items: [
                                {
                                    type: 'doc',
                                    id: 'deployment/installation/platform/install_on_apache_hadoop',
                                },
                            ],
                        },
                        {
                            type: 'doc',
                            id: 'deployment/installation/uninstallation',
                        },
                        {
                            type: 'doc',
                            id: 'deployment/installation/install_validation',
                        },
                    ],
                },
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
                            id: 'operations/project-operation/toolbar'
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
            link: {
                type: 'doc',
                id: 'modeling/intro'
            },
            items: [
                {
                    type: 'doc',
                    id: 'modeling/data_modeling'
                },
                {
                    type: 'doc',
                    id: 'modeling/manual_modeling'
                },
                {
                    type: 'doc',
                    id: 'modeling/model_concepts_operations'
                },
                {
                    type: 'category',
                    label: 'Advanced Mode Design',
                    link: {
                        type: 'doc',
                        id: 'modeling/model_design/intro'
                    },
                    items: [
                        {
                            type: 'category',
                            label: 'Measures',
                            link: {
                                type: 'doc',
                                id: 'modeling/model_design/measure_design/intro'
                            },
                            items: [
                                {
                                    type: 'doc',
                                    id: 'modeling/model_design/measure_design/topn'
                                },
                                {
                                    type: 'doc',
                                    id: 'modeling/model_design/measure_design/count_distinct_bitmap'
                                },
                                {
                                    type: 'doc',
                                    id: 'modeling/model_design/measure_design/count_distinct_hllc'
                                },
                                {
                                    type: 'doc',
                                    id: 'modeling/model_design/measure_design/percentile_approx'
                                },
                                {
                                    type: 'doc',
                                    id: 'modeling/model_design/measure_design/corr'
                                },
                                {
                                    type: 'doc',
                                    id: 'modeling/model_design/measure_design/collect_set'
                                },
                                {
                                    type: 'doc',
                                    id: 'modeling/model_design/measure_design/sum_expression'
                                },
                                {
                                    type: 'doc',
                                    id: 'modeling/model_design/measure_design/count_distinct_case_when_expr'
                                },
                            ],
                        },
                        {
                            type: 'doc',
                            id: 'modeling/model_design/computed_column'
                        },
                        {
                            type: 'doc',
                            id: 'modeling/model_design/slowly_changing_dimension'
                        },
                        {
                            type: 'doc',
                            id: 'modeling/model_design/aggregation_group'
                        },
                        {
                            type: 'doc',
                            id: 'modeling/model_design/table_index'
                        },
                        {
                            type: 'category',
                            label: 'Model Advanced Settings',
                            link: {
                                type: 'doc',
                                id: 'modeling/model_design/advance_guide/intro'
                            },
                            items: [
                                {
                                    type: 'doc',
                                    id: 'modeling/model_design/advance_guide/model_metadata_managment'
                                },
                                {
                                    type: 'doc',
                                    id: 'modeling/model_design/advance_guide/multilevel_partitioning'
                                },
                                {
                                    type: 'doc',
                                    id: 'modeling/model_design/advance_guide/fast_bitmap'
                                },
                                {
                                    type: 'doc',
                                    id: 'modeling/model_design/advance_guide/integer_encoding'
                                },
                            ],
                        },
                        {
                            type: 'doc',
                            id: 'modeling/model_design/precompute_join_relations'
                        },
                    ],
                },
                {
                    type: 'category',
                    label: 'Load Data',
                    link: {
                        type: 'doc',
                        id: 'modeling/load_data/intro'
                    },
                    items: [
                        {
                            type: 'doc',
                            id: 'modeling/load_data/full_build'
                        },
                        {
                            type: 'doc',
                            id: 'modeling/load_data/by_date'
                        },
                        {
                            type: 'doc',
                            id: 'modeling/load_data/build_index'
                        },
                        {
                            type: 'category',
                            label: 'Segment Operation and Settings',
                            link: {
                                type: 'doc',
                                id: 'modeling/load_data/segment_operation_settings/intro'
                            },
                            items: [
                                {
                                    type: 'doc',
                                    id: 'modeling/load_data/segment_operation_settings/segment_merge'
                                },
                            ],
                        },
                    ],
                },
            ],
        },
        {
            type: 'category',
            label: 'Monitor Job',
            link: {
                type: 'doc',
                id: 'monitor/intro'
            },
            items: [
                {
                    type: 'doc',
                    id: 'monitor/job_concept_settings'
                },
                {
                    type: 'doc',
                    id: 'monitor/job_operations'
                },
                {
                    type: 'doc',
                    id: 'monitor/job_diagnosis'
                },
                {
                    type: 'doc',
                    id: 'monitor/job_exception_resolve'
                },
            ],
        },
        {
            type: 'category',
            label: 'Query',
            link: {
                type: 'doc',
                id: 'query/intro'
            },
            items: [
                {
                    type: 'doc',
                    id: 'query/data_type'
                },
                {
                    type: 'category',
                    label: 'Basic Query Execution',
                    link: {
                        type: 'doc',
                        id: 'query/insight/intro'
                    },
                    items: [
                        {
                            type: 'doc',
                            id: 'query/insight/insight'
                        },
                        {
                            type: 'doc',
                            id: 'query/insight/sql_spec'
                        },
                        {
                            type: 'doc',
                            id: 'query/insight/async_query'
                        },
                    ],
                },
                {
                    type: 'doc',
                    id: 'query/history'
                },
                {
                    type: 'category',
                    label: 'Query Optimization',
                    link: {
                        type: 'doc',
                        id: 'query/optimization/intro'
                    },
                    items: [
                        {
                            type: 'doc',
                            id: 'query/optimization/query_enhanced'
                        },
                        {
                            type: 'doc',
                            id: 'query/optimization/segment_pruning'
                        },
                    ],
                },
                {
                    type: 'category',
                    label: 'Query PushDown',
                    link: {
                        type: 'doc',
                        id: 'query/pushdown/intro'
                    },
                    items: [
                        {
                            type: 'doc',
                            id: 'query/pushdown/pushdown_to_embedded_spark'
                        },
                    ],
                },
            ],
        },
        {
            type: 'category',
            label: 'Snapshot Management',
            link: {
                type: 'doc',
                id: 'snapshot/intro'
            },
            items: [
                {
                    type: 'doc',
                    id: 'snapshot/snapshot_management'
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
                    id: 'development/how_to_write_doc'
                },
                {
                    type: 'doc',
                    id: 'development/how_to_debug_kylin_in_ide'
                },
                {
                    type: 'doc',
                    id: 'development/how_to_test'
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
