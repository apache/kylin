// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Kylin 5.0 Alpha',
  tagline: 'Kylin 5.0 is a unified and powerful OLAP platform for Hadoop and Cloud.',
  url: 'https://kylin.apache.org',
  baseUrl: '/',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',
  favicon: 'img/favicon.ico',

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'hit-lacus', // Usually your GitHub org/user name.
  projectName: 'kylin', // Usually your repo name.
  deploymentBranch:'kylin5_doc',

  // Even if you don't use internalization, you can use this field to set useful
  // metadata like html lang. For example, if your site is Chinese, you may want
  // to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          editUrl:
            'https://github.com/apache/kylin/tree/kylin5/document/website/',
        },
        blog: {
          showReadingTime: true,
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          editUrl:
            'https://github.com/apache/kylin/tree/kylin5/document/website/',
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      navbar: {
        title: 'Apache Kylin 5.0 Alpha',
        logo: {
          alt: 'Kylin Logo',
          src: 'img/kylin_logo.png',
        },
        items: [
          {
            type: 'doc',
            docId: 'download',
            position: 'left',
            label: 'Download',
          },

          {
            type: 'doc',
            docId: 'intro',
            position: 'left',
            label: 'Document',
          },

          {
            type: 'doc',
            docId: 'development/how_to_contribute',
            position: 'left',
            label: 'Development',
          },

          {
            type: 'doc',
            docId: 'community',
            position: 'left',
            label: 'Community',
          },

          {to: '/blog', label: 'Blog', position: 'left'},

          {
            href: 'https://github.com/apache/kylin',
            label: 'GitHub',
            position: 'right',
          },
        ],
      },
      footer: {
        logo: {
          alt: 'Apache',
          src: 'img/feather-small.gif',
          href: 'https://apache.org',
          width: 160,
          height: 51,
        },
        style: 'light',
        links: [
          {
            title: 'Docs',
            items: [
              {
                label: 'Tutorial',
                to: '/docs/intro',
              },
            ],
          },
          {
            title: 'Community',
            items: [
              {
                label: 'JIRA',
                href: 'https://issues.apache.org/jira/projects/KYLIN/issues',
              },
              {
                label: 'Mailing List Archives',
                href: 'https://lists.apache.org/list.html?user@kylin.apache.org',
              },
              {
                label: 'Wiki',
                href: 'https://cwiki.apache.org/confluence/display/KYLIN/',
              },
              {
                label: 'Stack Overflow',
                href: 'https://stackoverflow.com/questions/tagged/kylin',
              }
            ],
          },
          {
            title: 'More',
            items: [
              {
                label: 'Blog',
                to: '/blog',
              },
              {
                label: 'GitHub Repo',
                href: 'https://github.com/apache/kylin',
              },
              {
                label: 'PowerBy',
                to: 'docs/powerBy',
              },
              {
                label: 'Doc of previous version',
                href: 'https://kylin.apache.org',
              },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} Apache Software Foundation under the terms of the Apache License v2.  Apache Kylin and its logo are trademarks of the Apache Software Foundation. 
        Built with Docusaurus.`,
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
      },
      docs: {
        sidebar: {
          autoCollapseCategories: true,
        }
      }
    }),
};

module.exports = config;
