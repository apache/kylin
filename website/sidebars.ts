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
    DocumentSideBar: [
        {
            type: 'doc',
            id: 'overview'
        },
        {
            type: 'category',
            label: 'Quick Start',
            link: {
                type: 'doc',
                id: 'quickstart/intro'
            },
            items: [
                {
                    type: 'doc',
                    id: 'quickstart/tutorial',
                }
            ],
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
                        label: 'Prerequisite',
                        link: {
                            type: 'doc',
                            id: 'deployment/prerequisite',
                        },
                        items: [
                            {
                                type: 'doc',
                                id: 'deployment/rdbms_metastore/use_mysql_as_metadb'
                            },
                            {
                                type: 'doc',
                                id: 'deployment/rdbms_metastore/usepg_as_metadb'
                            },
                        ]
                    },
                    {
                        type: 'doc',
                        id: 'deployment/single_node_mode'
                    },
                    {
                        type: 'doc',
                        id: 'deployment/cluster_mode'
                    },
                    {
                        type: 'doc',
                        id: 'deployment/rw_separation'
                    },
                    {
                        type: 'category',
                        label: 'Configuration',
                        link: {
                            type: 'doc',
                            id: 'configuration/intro',
                        },
                        items: [
                            {
                                type: 'doc',
                                id: 'configuration/config'
                            },
                            {
                                type: 'doc',
                                id: 'configuration/gluten_config'
                            },
                            {
                                type: 'doc',
                                id: 'configuration/hadoop_queue_config'
                            },
                            {
                                type: 'doc',
                                id: 'configuration/https'
                            },
                            {
                                type: 'doc',
                                id: 'configuration/log_rotate'
                            },
                            {
                                type: 'doc',
                                id: 'configuration/query_cache'
                            },
                            {
                                type: 'doc',
                                id: 'configuration/spark_dynamic_allocation'
                            },
                            {
                                type: 'doc',
                                id: 'configuration/spark_rpc_encryption'
                            },
                        ],
                    },
            ],
        },
        {
            type: 'category',
            label: 'Datasource',
            link: {
                type: 'doc',
                id: 'datasource/intro',
            },
            items: [
                {
                    type: 'doc',
                    id: 'datasource/import_hive'
                },
                {
                    type: 'doc',
                    id: 'datasource/data_sampling'
                },
                {
                    type: 'doc',
                    id: 'datasource/logical_view'
                },
            ],
        },
        {
            type: 'category',
            label: 'Internal Table',
            link: {
                type: 'doc',
                id: 'internaltable/intro',
            },
            items: [
                {
                    type: 'doc',
                    id: 'internaltable/internal_table_management'
                },
                {
                    type: 'doc',
                    id: 'internaltable/load_data_into_internal_table'
                },
                {
                    type: 'doc',
                    id: 'internaltable/query_internal_table'
                }
            ],
        },
        //////////////////////////////////////// Model category \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
        {
            type: 'category',
            label: 'Model',
            link: {
                type: 'doc',
                id: 'model/intro'
            },
            items: [
                {
                    type: 'category',
                    label: 'Manual Modeling',
                    link: {
                        type: 'doc',
                        id: 'model/manual/modeling'
                    },
                    items: [
                        {
                            type: 'doc',
                            id: 'model/manual/aggregation_group'
                        },
                        {
                            type: 'doc',
                            id: 'model/manual/table_index'
                        },
                        {
                            type: 'doc',
                            id: 'model/manual/computed_column'
                        },
                    ]
                },
                {
                    type: 'category',
                    label: 'Recommendation',
                    link: {
                        type: 'doc',
                        id: 'model/rec/intro'
                    },
                    items: [
                        {
                            type: 'doc',
                            id: 'model/rec/sql_modeling'
                        },
                        {
                            type: 'doc',
                            id: 'model/rec/optimize_by_qh'
                        },
                        {
                            type: 'doc',
                            id: 'model/rec/index_optimization'
                        },
                    ],
                },
                {
                    type: 'category',
                    label: 'Management',
                    items: [
                        {
                            type: 'doc',
                            id: 'model/manage/model_info'
                        },
                        {
                            type: 'doc',
                            id: 'model/manage/model_operation'
                        },
                        {
                            type: 'doc',
                            id: 'model/manage/import_export'
                        },
                        {
                            type: 'doc',
                            id: 'model/manage/data_loading'
                        },
                        {
                            type: 'doc',
                            id: 'model/manage/segment'
                        },
                        /*{
                            type: 'doc',
                            id: 'model/manage/configuration'
                        }*/
                    ],
                },
                {
                    type: 'doc',
                    id: 'model/snapshot/snapshot'
                },
                {
                    type: 'category',
                    label: 'Advanced Features',
                    items: [
                        {
                            type: 'category',
                            label: 'Measures',
                            link: {
                                type: 'doc',
                                id: 'model/features/measures/intro'
                            },
                            items: [
                                {
                                    type: 'doc',
                                    id: 'model/features/measures/topn'
                                },
                                {
                                    type: 'doc',
                                    id: 'model/features/measures/count_distinct_bitmap'
                                },
                                {
                                    type: 'doc',
                                    id: 'model/features/measures/count_distinct_hllc'
                                },
                                {
                                    type: 'doc',
                                    id: 'model/features/measures/percentile_approx'
                                },
                                {
                                    type: 'doc',
                                    id: 'model/features/measures/corr'
                                },
                                {
                                    type: 'doc',
                                    id: 'model/features/measures/collect_set'
                                },
                            ],
                        },
                        {
                            type: 'doc',
                            id: 'model/features/scd2'
                        },
                        // {
                        //     type: 'doc',
                        //     id: 'model/features/excluded_column'
                        // },
                        {
                            type: 'doc',
                            id: 'model/features/runtime_join'
                        },
                        {
                            type: 'doc',
                            id: 'model/features/multi_partitioning'
                        },
                        {
                            type: 'doc',
                            id: 'model/features/scalar_subquery'
                        },
                        {
                            type: 'doc',
                            id: 'model/features/fast_bitmap'
                        },
                        {
                            type: 'doc',
                            id: 'model/features/integer_encoding'
                        },
                    ],
                },
                {
                    type: 'category',
                    label: 'Streaming',
                    link: {
                        type: 'doc',
                        id: 'model/streaming/intro'
                    },
                    items: [
                        {
                            type: 'doc',
                            id: 'model/streaming/prerequisite'
                        },
                        {
                            type: 'doc',
                            id: 'model/streaming/real-time'
                        },
                        {
                            type: 'doc',
                            id: 'model/streaming/custom-parser'
                        },
                    ],
                },
            ],
        },
        //////////////////////////////////////// Query Category \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
        {
            type: 'category',
            label: 'Query',
            link: {
                type: 'doc',
                id: 'query/insight'
            },
            items: [
                {
                    type: 'category',
                    label: 'SQL Specification',
                    link: {
                        type: 'doc',
                        id: 'query/specification/sql_spec'
                    },
                    items: [
                        {
                            type: 'doc',
                            id: 'query/specification/data_type'
                        },
                        {
                            type: 'doc',
                            id: 'query/specification/operators'
                        },
                        {
                            type: 'doc',
                            id: 'query/specification/functions'

                        },
                        {
                            type: 'category',
                            label: 'Advanced Functions',
                            items: [
                                {
                                    type: 'doc',
                                    id: 'query/specification/window_function',
                                },
                                {
                                    type: 'doc',
                                    id: 'query/specification/grouping_function',
                                },
                                {
                                    type: 'doc',
                                    id: 'query/specification/bitmap_function',
                                },
                                {
                                    type: 'doc',
                                    id: 'query/specification/intersect_function',
                                },
                            ]
                        },
                    ]
                },
                {
                    type: 'category',
                    label: 'Query Tuning',
                    items: [
                        {
                            type: 'doc',
                            id: 'query/tuning/data_skipping'
                        },
                        {
                            type: 'doc',
                            id: 'query/tuning/model_priority'
                        },
                        /*{
                            type: 'doc',
                            id: 'query/tuning/partial_match_join'
                        },*/
                        {
                            type: 'doc',
                            id: 'query/tuning/sum_expression'
                        },
                        {
                            type: 'doc',
                            id: 'query/tuning/count_distinct_expr'
                        },
                        {
                            type: 'doc',
                            id: 'query/tuning/query_enhanced'
                        },
                    ],
                },
                {
                    type: 'doc',
                    id: 'query/push_down'
                },
                {
                    type: 'category',
                    label: 'Query History',
                    items: [
                        {
                            type: 'doc',
                            id: 'query/history/query_history'
                        },
                        {
                            type: 'doc',
                            id: 'query/history/query_history_fields'
                        },
                    ],
                },

                {
                    type: 'doc',
                    id: 'query/async_query'
                },
            ],
        },
        {
            type: 'category',
            label: 'Maintenance Guide',
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
                    label: 'Project Managing',
                    link: {
                        type: 'doc',
                        id: 'operations/project-managing/intro'
                    },
                    items: [
                        {
                            type: 'doc',
                            id: 'operations/project-managing/project_management'
                        },
                        {
                            type: 'doc',
                            id: 'operations/project-managing/project_settings'
                        },
                        {
                            type: 'doc',
                            id: 'operations/project-managing/toolbar'
                        },
                        {
                            type: 'doc',
                            id: 'operations/project-managing/alerting'
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
                                    id: 'operations/system-operation/cli_tool/environment_check',
                                },
                                {
                                    type: 'doc',
                                    id: 'operations/system-operation/cli_tool/diagnosis'
                                },
                                {
                                    type: 'doc',
                                    id: 'operations/system-operation/cli_tool/metadata_operation'
                                },
                                {
                                    type: 'doc',
                                    id: 'operations/system-operation/cli_tool/rollback'
                                },
                            ],
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
                    label: 'Job Monitoring',
                    link: {
                        type: 'doc',
                        id: 'operations/job-monitoring/intro'
                    },
                    items: [
                        {
                            type: 'doc',
                            id: 'operations/job-monitoring/job_concept_settings'
                        },
                        {
                            type: 'doc',
                            id: 'operations/job-monitoring/job_operations'
                        },
                        {
                            type: 'doc',
                            id: 'operations/job-monitoring/job_diagnosis'
                        },
                        {
                            type: 'doc',
                            id: 'operations/job-monitoring/job_exception_resolve'
                        },
                    ],
                },
                {
                    type: 'category',
                    label: 'System Monitoring',
                    link: {
                        type: 'doc',
                        id: 'operations/system-monitoring/intro',
                    },
                    items: [
                        {
                            type: 'category',
                            label: 'InfluxDB',
                            link: {
                                type: 'doc',
                                id: 'operations/system-monitoring/influxdb/intro',
                            },
                            items: [
                                {
                                    type: 'doc',
                                    id: 'operations/system-monitoring/influxdb/influxdb'
                                },
                                {
                                    type: 'doc',
                                    id: 'operations/system-monitoring/influxdb/influxdb_maintenance'
                                },
                            ],
                        },
                        {
                            type: 'doc',
                            id: 'operations/system-monitoring/metrics_intro',
                        },
                        {
                            type: 'doc',
                            id: 'operations/system-monitoring/service'
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
            label: 'Integration',
            link: {
                type: 'doc',
                id: 'integration/intro',
            },
            items: [
                {
                  type: 'doc',
                  id: 'integration/jdbc',
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
                    label: 'Model Management',
                    link: {
                        type: 'doc',
                        id: 'restapi/model_api/intro',
                    },
                    items: [
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
                    id: 'restapi/custom_jar_manager_api'
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
                        // {
                        //     type: 'doc',
                        //     id: 'restapi/acl_api/acl_api'
                        // },
                    ],
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
                    id: 'development/coding_convention'
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
                    id: 'development/how_to_debug_kylin_in_local'
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
                    id: 'development/how_to_subscribe_mailing_list'
                },
                {
                    type: 'doc',
                    id: 'development/how_to_understand_kylin_design'
                },
                {
                    type: 'doc',
                    id: 'development/security'
                }

            ],
        },
    ],
    CommunitySideBar: [
        {
            type: 'doc',
            id: 'community',
            label: 'Community',
        },
        {
            type: 'doc',
            id: 'poweredby',
            label: 'Use Cases',
        },
    ],
    DownloadSideBar: [
        {
            type: 'doc',
            id: 'download',
            label: 'Download',
        },
    ],
};

export default sidebars;
