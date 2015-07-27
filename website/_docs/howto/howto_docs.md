---
layout: docs
title:  How to write document
categories: howto
permalink: /docs/howto/howto_docs.html
version: v0.7.2
since: v0.7.2
---

## Before your work

Install following tools before you add or edit documentation:  
1. First, make sure Ruby and Gem are works on your machine  
_For Mac User, please refer [this](https://github.com/sstephenson/rbenv#homebrew-on-mac-os-x) to setup ruby env._  
2. Then, install [Jekyll](http://jekyllrb.com), and plugins, for example:  
`gem install jekyll jekyll-multiple-languages kramdown rouge`  
3. [Jekyll Multiple Languages Plugin](http://jekyll-langs.liaohuqiu.net/cn/)  
4. And any markdown editor

## About Jekyll
Jekyll is a Ruby script to generate a static HTML website from source text and themes, the HTML is generated before being deployed to the web server. Jekyll also happens to be the engine behind GitHub Pages.

Here are good reference about basic usage of Jekyll: [Learning Jekyll By Example](http://learn.andrewmunsell.com/learn/jekyll-by-example/tutorial)

Apache Kylin's website and documentation is using Jekyll to manage and generate final content which avaliable at [http://kylin.incubator.apache.org](http://kylin.incubator.apache.org).

## Multi-Language
To draft Chinese version document or translate existing one, just add or copy that doc and name with .cn.md as sufffix. It will generate under /cn folder with same name as html file.  
To add other language, please update _config.yml and follow the same pattern as Chinese version.

# Kylin document sturcture and navigation menu

1. __Home Page__: _"index.md"_ Home page of Docs
2. __Getting Started__: _"gettingstarted"_ General docs about Apache Kylin, including FAQ, Terminology
3. __Installation__: _"install"_ Apache Kylin installation guide
4. __Tutorial__: _"tutorial"_ User tutorial about how to use Apache Kylin
5. __How To__: _"howto"_ Guide for more detail help
6. __Development__: _"development"_ For developer to contribute, integration with other application and extend Apache Kylin
7. __Others__: Other docs.

The menu is managed by Jekyll collection:

* ___data/docs.yml__: English version menu structure  
* ___data/docs-cn.yml__: Chinese version menu structure   
* __add new menu item__: To add new item: create new docs under relative folder, e.g howto_example.md. add following Front Mark:  

```
---
layout: docs
title:  How to expamle
categories: howto
permalink: /docs/howto/howto_example.html
version: v0.7.2
since: v0.7.2
---
```

change the __permalink__ to exactly link   
Then add item to docs.yml like:

```
- title: How To
  docs:
  - howto/howto_contribute
  - howto/howto_jdbc
  - howto/howto_example
```

# How to edit document
Open doc with any markdown editor, draft content and preview in local.

Sample Doc:

```
---
layout: docs
title:  How to example
categories: howto
permalink: /docs/howto/howto_example.html
version: v0.7.2
since: v0.7.2
---

## This is example doc
The quick brown fox jump over the lazy dog.

```

# How to add image
All impage please put under _images_ folder, in your document, please using below sample to include image:  

```
![](/images/Kylin-Web-Tutorial/2 tables.png)

```

# How to add link
Using relative path for site links, for example:

```
[REST API](docs/development/rest_api.html). 

```

# How to add code highlight
We are using [Rouge](https://github.com/jneen/rouge) to highlight code syntax.
check this doc's source code for more detail sample.

# How to preview in your local
You can preview in your markdown editor, to check exactly what it will looks like on website, please run Jekyll from "website" folder:  
```
jekyll s

```
Then access http://127.0.0.1:4000 in your browser.

## How to publish to website (for committer only)  

### Setup

1. `cd website`
2. `svn co https://svn.apache.org/repos/asf/incubator/kylin/site _site`
3. `sudo apt-get install rubygems ruby2.1-dev zlib1g-dev` (linux)
4. `sudo gem install bundler github-pages jekyll`
5. `bundle install`

___site__ folder is working dir which will be removed anytime by maven or git, please make sure only check out from svn when you want to publish to website.

### Running locally  
Before opening a pull request or push to git repo, you can preview changes from your local box with following:

1. `cd website`
2. `jekyll s`
3. Open [http://127.0.0.1:4000](http://127.0.0.1:4000) in your browser

### Pushing to site 

1. `cd website/_site`
2. `svn status`
3. You'll need to `svn add` any new files
4. `svn commit -m 'UPDATE MESSAGE'`

Within a few minutes, svnpubsub should kick in and you'll be able to
see the results at
[http://kylin.incubator.apache.org](http://kylin.incubator.apache.org/).


