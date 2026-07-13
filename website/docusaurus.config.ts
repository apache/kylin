// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion
import VersionsArchived from './versionsArchived.json';
import versions from './versions.json';
import NodeRequire from '@types/node/globals'
import {themes, type PrismTheme} from 'prism-react-renderer';

const lightCodeTheme = themes.github

const ArchivedVersionsDropdownItems = Object.entries(VersionsArchived).splice(
  0,
  5,
);

function isPrerelease(version: string) {
  return (
    version.includes('-') ||
    version.includes('alpha') ||
    version.includes('beta') ||
    version.includes('rc')
  );
}

function getLastStableVersion() {
  // const lastStableVersion = versions.find((version) => !isPrerelease(version));
  const lastStableVersion = '5.0.4';
  if (!lastStableVersion) {
    throw new Error('unexpected, no stable Docusaurus version?');
  }
  return lastStableVersion;
}

const announcedVersion = getAnnouncedVersion();

function getLastStableVersionTuple(): [string, string, string] {
  const lastStableVersion = getLastStableVersion();
  const parts = lastStableVersion.split('.');
  if (parts.length !== 3) {
    throw new Error(`Unexpected stable version name: ${lastStableVersion}`);
  }
  return [parts[0]!, parts[1]!, parts[2]!];
}

function getAnnouncedVersion() {
  const [major, minor] = getLastStableVersionTuple();
  return `${major}.${minor}`;
}

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Apache Kylin',
  tagline: 'Kylin is a high concurrency, high performance and intelligent OLAP engine that provides low-cost and ultimate data analytics experience.',
  url: 'https://kylin.apache.org',
  baseUrl: '',
  onBrokenLinks: 'warn',
  onBrokenMarkdownLinks: 'warn',
  favicon: 'img/favicon.ico',

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'apache', // Usually your GitHub org/user name.
  projectName: 'kylin', // Usually your repo name.
  deploymentBranch: 'kylin5_doc',

  // add search plugin

  plugins: [
    [
      require.resolve("@easyops-cn/docusaurus-search-local"),
      {
        // Options here
        // whether to index docs pages
        indexDocs: true,

        // whether to index blog pages
        indexBlog: true,

        // whether to index static pages
        // /404.html is never indexed
        indexPages: false,

        hashed: true,
        language: ["en", "zh"],

      },
    ],
  ],

  // Even if you don't use internalization, you can use this field to set useful
  // metadata like html lang. For example, if your site is Chinese, you may want
  // to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    // locales: ['en'],
    locales: ['en', 'zh-Hans'],
    // path: 'i18n',
    localeConfigs: {
      'en': {
        label: 'English',
        htmlLang: 'en-GB',
      },
      'zh-Hans': {
        label: '简体中文',
        path: 'zh-Hans',
      },
    },
  },

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: './sidebars.ts',
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          editUrl:
            'https://github.com/apache/kylin/tree/doc5.0/website/',
          lastVersion: 'current',
          versions: {
            current: {
              label: '5.0.4',
              badge: true,
              path: '/',
              banner: 'none',
            },
            '4.0.4': {
              label: '4.0.4',
              badge: true,
              path: '/4.0.4/',
              banner: 'unmaintained'
            },
            '3.1.3': {
              label: '3.1.3',
              badge: true,
              path: '/3.1.3/',
              banner: 'unmaintained'
            },
            '2.4.0': {
              label: '2.4.0',
              badge: true,
              path: '/2.4.0/',
              banner: 'unmaintained'
            },
          },
          showLastUpdateAuthor: false,
          showLastUpdateTime: true,
        },
        blog: {
          blogSidebarTitle: 'Technical Blogs',
          blogDescription: 'Technical blogs for Kylin 5.0',
          postsPerPage: 'ALL',
          blogSidebarCount: 20,
          showReadingTime: true,
          readingTime: ({content, frontMatter, defaultReadingTime}) =>
            defaultReadingTime({content, options: {wordsPerMinute: 100}}),
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          editUrl:
            'https://github.com/apache/kylin/tree/doc5.0/website/',
        },
        theme: {
          customCss: ['./src/css/custom.css'],
        },
      }),
    ],
  ],

  headTags: [
    {
      tagName: 'meta',
      attributes: {
        httpEquiv: 'Content-Security-Policy',
        content: "img-src 'self' data: https://github.com https://*.apache.org/ https://avatars.githubusercontent.com;",
      },
    },
  ],

  scripts: [
    {
      src: 'https://buttons.github.io/buttons.js',
      async: true,
      defer: true,
    },
  ],
  themeConfig:
  /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      colorMode: {
        defaultMode: 'light',
        disableSwitch: true,
      },
      navbar: {
        title: 'Apache Kylin',
        logo: {
          alt: 'Kylin Logo',
          src: 'img/kylin/SVG/Apache-Kylin-blue.svg',
        },
        items: [
          {
            type: 'docsVersionDropdown',
            position: 'right',
            dropdownActiveClassDisabled: true,
            dropdownItemsAfter: [
              //   {
              //     type: 'html',
              //     value: '<hr class="dropdown-separator">',
              //   },
              // ...ArchivedVersionsDropdownItems.map(
              //   ([versionName, versionUrl]) => ({
              //     label: versionName,
              //     href: versionUrl,
              //   }),
              // ),
              // {
              //   type: 'html',
              //   value: '<hr class="dropdown-separator">',
              // },
              {
                to: 'docs/release_notes',
                label: 'Release Notes',
              },
            ]
          },
          {
            type: 'doc',
            docId: 'overview',
            position: 'left',
            label: 'Documentation',
          },
          {
            type: 'doc',
            docId: 'community',
            position: 'left',
            label: 'Community',
          },
          {
            type: 'doc',
            docId: 'development/intro',
            position: 'left',
            label: 'Development',
          },
          {
            type: 'localeDropdown',
            position: 'right',
          },
          {
            type: 'doc',
            docId: 'download',
            position: 'right',
            label: 'Download',
          },
          {
            to: '/blog',
            label: 'Blogs',
            position: 'left'
          },
          {
            href: 'https://github.com/apache/kylin',
            position: 'right',
            className: 'header-github-link',
            'aria-label': 'GitHub repository',
          },
          {
            type: 'search',
            position: 'right',
          }
        ],
      },
      announcementBar: {
        id: `announcementBar-v${announcedVersion}`,
        backgroundColor: '#153E7B',
        textColor: '#FFFFFF',
        content: `⭐️ <b><a target="_blank" href="http://kylin.apache.org/docs/release_notes">Apache Kylin ${announcedVersion}</a> is released! 🎉 If you like Kylin, give it a star on <a target="_blank" rel="noopener noreferrer" href="https://github.com/apache/kylin">GitHub</a>!</b> ❤️`,
      },
      footer: {
        logo: {
          alt: 'Apache',
          src: 'img/asf_logo.svg',
          href: 'https://apache.org',
          width: 160,
          height: 51,
        },
        style: 'dark',
        links: [
          {
            title: 'Docs',
            items: [
              {
                label: 'Quick Start',
                to: '/docs/quickstart/tutorial',
              },
              {
                label: 'How to contribute',
                to: '/docs/development/how_to_contribute',
              },
              {
                label: 'Roadmap',
                to: 'blog/roadmap_of_kylin_50_cn',
              }
            ]
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
                label: 'Blogs',
                to: '/blog',
              },
              {
                label: 'Source Code',
                href: 'https://github.com/apache/kylin',
              }
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} Apache Software Foundation under the terms of the Apache License v2.
        <br>Apache Kylin and its logo are trademarks of the Apache Software Foundation.`,
      },
      prism: {
        additionalLanguages: ['java'],
        theme: lightCodeTheme,
      },
      docs: {
        sidebar: {
          autoCollapseCategories: true,
        }
      }
    }),
};

export default config;
