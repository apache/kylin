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
  KylinDocumentSideBar:[
      {
          type: 'doc',
          id: 'intro',
          label: 'Introduction to Kylin 5'
      },
      {
          type: 'doc',
          id: 'download',
          label: 'Download',
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
          items: [
              {
                  type: 'doc',
                  id: 'deployment/intro'
              },
              {
                  type: 'doc',
                  id: 'deployment/prerequisite'
              },
              {
                  type: 'doc',
                  id: 'deployment/network_port_requirements'
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
                  label: 'Use RDBMS as Metastore',
                  link: {
                      type: 'doc',
                      id: 'deployment/rdbms_metastore/intro',
                  },
                  items: [
                      {
                          type: 'category',
                          label: 'MySQL',
                          link: {
                              type: 'doc',
                              id: 'deployment/rdbms_metastore/mysql/intro',
                          },
                          items: [
                              {
                                  type: 'doc',
                                  id: 'deployment/rdbms_metastore/mysql/install_mysql'
                              },
                              {
                                  type: 'doc',
                                  id: 'deployment/rdbms_metastore/mysql/mysql_metastore'
                              },
                          ],
                      },
                      {
                          type: 'category',
                          label: 'PostgreSQL',
                          link: {
                              type: 'doc',
                              id: 'deployment/rdbms_metastore/postgresql/intro',
                          },
                          items: [
                              {
                                  type: 'doc',
                                  id: 'deployment/rdbms_metastore/postgresql/install_postgresql'
                              },
                              {
                                  type: 'doc',
                                  id: 'deployment/rdbms_metastore/postgresql/default_metastore'
                              },
                          ],
                      },
                  ]
              },
              {
                  type: 'category',
                  label: 'Deployment Mode',
                  link: {
                      type: 'doc',
                      id: 'deployment/deploy_mode/intro',
                  },
                  items: [
                      {
                          type: 'doc',
                          id: 'deployment/deploy_mode/cluster_deployment'
                      },
                      {
                          type: 'doc',
                          id: 'deployment/deploy_mode/service_discovery'
                      },
                      {
                          type: 'doc',
                          id: 'deployment/deploy_mode/rw_separation'
                      },
                  ]
              }
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
                  id: 'restapi/segment_managment_api'
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
      {
          type: 'category',
          label: 'Operation and Maintenance Guide',
          link: {
              type: 'doc',
              id: 'operations/intro',
          },
          items: [
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
                  ],
              },
          ],
      },
      {
          type: 'category',
          label: 'Configuration Guide',
          items: [
              {
                  type: 'doc',
                  id: 'configuration/intro'
              },
          ],
      },
      {
          type: 'category',
          label: 'Development Guide',
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
                  id: 'development/how_to_develop'
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
      {
          type: 'doc',
          id: 'community',
          label: 'Community',
      },
      {
          type: 'doc',
          id: 'powerBy',
          label: 'PowerBy',
      }
  ],
};

module.exports = sidebars;
