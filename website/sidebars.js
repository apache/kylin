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
                          type: 'doc',
                          id: 'deployment/rdbms_metastore/default_metastore'
                      },
                      {
                          type: 'doc',
                          id: 'deployment/rdbms_metastore/install_postgresql'
                      },
                      {
                          type: 'doc',
                          id: 'deployment/rdbms_metastore/install_mysql'
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
          items: [
              {
                  type: 'doc',
                  id: 'restapi/intro'
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
          label: 'Operations',
          items: [
              {
                  type: 'doc',
                  id: 'operations/intro'
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
