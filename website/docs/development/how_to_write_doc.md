---
title: How to write document
language: en
sidebar_label: How to write document
pagination_label: How to write document
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: development/how_to_contribute
pagination_next: development/how_to_debug_kylin_in_ide
keywords:
    - doc
    - document
draft: false
last_update:
    date: 08/24/2022
    author: Tengting Xu, Xiaoxiang Yu
---

From Kylin 5.0, Kylin documents are written using [Docusaurus](https://docusaurus.io/). Please note multi-version and i18n (multi-language) is not supported right now, but is in the plan. Contributions are very much appreciated.

### Shortcut: Edit a single existing page

:::info Shortcut editing a single page
1. This shortcut is extreme useful if you found some minor typos or mistakes on a single page, you can edit the document in browser right away in a few minutes without preparation.
2. But if the change is more complex, like add/edit several pages, upload images, or change global config files, please jump to next paragraph: [**Before your work**](#Before_your_work).
:::

1. Just scroll down the page to the bottom and click the `Edit this page`.
![](images/how-to-write-doc-01.png)

2. Edit this file in browser.
![](images/how-to-write-doc-03.png)

3. Propose your changes by raising a pull request.
![](images/how-to-write-doc-04.png)

4. And you are done! The changes proposed will get reviewed by a reviewer, and if OK, will be merged and be seen by the community.

### <span id="Before_your_work">Before your work</span>

Before adding new documentation, it is best to setup the preview environment first.

1. Install Node.js

   Make sure [Node.js](https://nodejs.org/en/download/) version is 16.14 or above by checking `node -v`. You can use [nvm](https://github.com/nvm-sh/nvm) for managing multiple Node versions on a single machine installed.

   ```shell
   node -v
   ```

   :::note Tips
   When installing Node.js via *Windows/macOS Installer*, recommend to check all checkboxes related to dependencies.
   :::

2. Clone the kylin doc branch

   ```shell
   cd /path/you/prefer/
   git clone --branch doc5.0 https://github.com/apache/kylin.git # Or git clone -b doc5.0 https://github.com/apache/kylin.git
   ```

3. Install the website dependencies

   ```shell
   cd /path/you/prefer/
   cd website
   npm install
   ```

   :::note Slow NPM in China?
   Add following lines to `~/.npmrc` and npm shall become much faster in China.
   ```
   sass_binary_site=https://npm.taobao.org/mirrors/node-sass/
   phantomjs_cdnurl=https://npm.taobao.org/mirrors/phantomjs/
   electron_mirror=https://npm.taobao.org/mirrors/electron/
   registry=https://registry.npm.taobao.org
   ```
   :::

   :::note Troubleshooting
   Depending on your OS environment, `npm install` can hit various issues at this stage and most of them are due to missing a certain library. Below are a few examples from a Ubuntu user.
   - If hit error `../src/common.cc:24:10: fatal error: vips/vips8: No such file or directory`
     - Try install glib2.0-dev, like `sudo apt-get install glib2.0-dev`
   - If hit error `Error: Command failed: /bin/sh -c autoreconf -ivf`
     - Try install autoconf, like `sudo apt-get install autoconf`
   :::
   
   For more information about [Docusaurus](https://docusaurus.io/), please refer to [Docusaurus Installation](https://docusaurus.io/docs/installation).

4. Launch the doc website and preview it locally

   ```shell
   npm run start
   ```

   The homepage of this doc site `http://localhost:3000` shall automatically open in your default browser if no error occurs. Modify any MD or resource file in your local repository, the changes shall reflect immediately in the browser. Very convenient for doc development.

### How to create new document

#### Step 1: Create a new markdown file with metadata

Create a new markdown file with any text editor, copy and paste following **Head Metadata Template** to the top your file. After that, replace the variables like `${TITLE OF NEW DOC}` with actual values.

```
---
title: ${TITLE OF NEW DOC}
language: en
sidebar_label: ${TITLE OF NEW DOC}
pagination_label: ${TITLE OF NEW DOC}
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - ${KEYWORD OF NEW DOC}
draft: false
last_update:
    date: ${DATE of YOUR COMMIT}
    author: ${YOUR FULL NAME}
---
```

:::info Head metadata?
___Head metadata___ is REQUIRED for all doc files. For more information, please refer to [docusaurus head metadata](https://docusaurus.io/docs/markdown-features/head-metadata).
:::

#### Step 2: Add content for your new doc

Add text in the [markdown format](https://docusaurus.io/docs/markdown-features).

Pictures usually go into a subfolder called `images`.

#### Step 3: Add new page to the sidebar

Sidebar contains the menu and the navigation tree of the doc site structure. It is maintained in a JS file located at `website/sidebars.js`.

For example, if you want to add a new doc `how_to_write_doc.md` to be the child of the `development` menu. Open the `sidebars.js` and modify the `DevelopmentSideBar` block. Add a new block at the tail of `items` of `DevelopmentSideBar`.

```shell
DevelopmentSideBar: [
    {
        ...
        items: [
            {...},
            ...,
            {
                type: 'doc',
                id: 'development/how_to_write_doc'
            },
        ],
    },
],              
```


#### Step 4: Preview the result locally

Saving all the changes, you can preview the result immediately in your browser. Please run following commands in the `website` directory, and a local version of the doc site shall show up in your browser `http://localhost:3000`.

```shell
cd website
npm run start
```

:::info Check your doc
- [ ] Whether the **look and feel** meet expectation?
- [ ] Whether the **link/pictures** work fine?
- [ ] Whether the important info is properly **highlighted**? [How to highlight?](#highlight_paragraph)
- [ ] Whether the **title levels** follow the [heading guide](#heading_level)?
:::

#### Step 5: Create a pull request

When everything looks fine, create a pull request to the [kylin doc5.0 branch](https://github.com/apache/kylin/tree/doc5.0).

:::note What, Pull Request?
For those who are new to pull requests, here it is explained.
- How to geek -- [What are git pull requests](https://www.howtogeek.com/devops/what-are-git-pull-requests-and-how-do-you-use-them/)
- Github -- [About pull requests](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/about-pull-requests)
:::

----

### Documentation Specification

#### About [Docusaurus](https://docusaurus.io/)

[Docusaurus](https://docusaurus.io/) is a static-site generator. It builds a single-page application with fast client-side navigation, leveraging the full power of React to make your site interactive. It provides out-of-the-box documentation features but can be used to create any kind of site (personal website, product, blog, marketing landing pages, etc).

Apache Kylin's website and documentation is using [Docusaurus](https://docusaurus.io/) to manage and generate final content which avaliable at [http://kylin.apache.org](http://kylin.apache.org).

#### Kylin document structure and navigation menu

The Kylin [website material](https://github.com/apache/kylin/tree/doc5.0) is maintained under the `doc5.0` branch.

1. __Home Page__: Home page of Docs
2. __Document__: General docs about Apache Kylin, including _Installation_, _Tutorial_, etc.
3. __Development__: _"development"_ For developer to contribute, to develop, integration with other application and extend Apache Kylin
4. __Download__: _"Download"_ Apache Kylin packages
5. __Community__: Apache kylin Community information
6. __Blog__: Engineering blogs about Apache Kylin

#### Full doc structure

The full doc structure about the newest Apache Kylin:

```shell
doc5.0
.
├── README.md
├── babel.config.js
├── blog
│ ├── ...
├── docs
│ ├── community.md
│ ├── configuration
│ │ ├── ...
│ ├── datasource
│ │ ├── ...
│ ├── deployment
│ │ ├── ...
│ ├── development
│ │ ├── ...
│ ├── download.md
│ ├── integration
│ │ └── intro.md
│ ├── intro.md
│ ├── modeling
│ │ ├── ...
│ ├── monitor
│ │ ├── ...
│ ├── operations
│ │ ├── ...
│ ├── powerBy.md
│ ├── query
│ │ ├── ...
│ ├── quickstart
│ │ ├── ...
│ ├── restapi
│ │ ├── ...
│ ├── snapshot
│ │ ├── ...
│ └── tutorial
│     ├── ...
├── docusaurus.config.js
├── package.json
├── sidebars.js
├── src
│ ├── components
│ │ └── ...
│ ├── css
│ │ └── ...
│ └── pages
│     ├── ...
├── static
│ └── img
│     ├── ...
```

More details about structure which managed by Docusaurus, please refer to [Project structure rundown](https://docusaurus.io/docs/installation#project-structure-rundown).

#### <span id="heading_level">Title/Heading Level</span>

Here is [official guide about heading](https://docusaurus.io/docs/markdown-features/toc#markdown-headings).  Please use level 3 title("###") and level 4 title("####") in most of the article.

Following is a general guide:
- Use level 2 heading(aka "##") as **top level** title. The number of top level title should not more than two. 
- Use level 3 heading(aka "###") as **middle level** title. 
- Use level 4 heading(aka "####") as **the lowest level** title.


We recommend you to check for [this article](how_to_contribute) for example. Following is toc of it.
```
## Guide for Contributor
### Detailed Description
#### Step 1: Fork Apache Kylin Repo
#### Step 2: Clone the fork repo
#### Step 3: xxx
#### Step 4: xxx
...

## Guide for Reviewer
### Code Review Guideline
### Patch +1 Policy
...
```

#### Sidebar
The Sidebar is managed by __sidebars.js__ , please refer to [Sidebar](https://docusaurus.io/docs/sidebar).

#### How to add image in doc
All image should be put under _images_ folder, in your document, please using below sample to include image:

```
![](images/how-to-write-doc-01.png)
```

#### How to link to another page
Using relative path for site links, check this [Markdown links](https://docusaurus.io/docs/markdown-features/links)


#### How to add source code in doc
We are using [Code Block](https://docusaurus.io/docs/markdown-features/code-blocks) to highlight code syntax, check this doc for more detail sample.

#### <span id="highlight_paragraph">How to highlight a sentence/paragraph</span>
We recommend you to use [admonitions feature](https://docusaurus.io/docs/markdown-features/admonitions) to highlight a sentence/paragraph, following is a example:

```
:::caution
Some **content** with _Markdown_ `syntax`. Check [this `api`](#).
:::
```
