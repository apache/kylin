---
layout: dev-cn
title:  如何写文档
categories: development
permalink: /cn/development/howto_docs.html
---

我们以 MD 格式编写文档并使用 [Jekyll](http://jekyllrb.com) 转换为 HTML。Jekyll 生成的 HTML 上传到 apache SVN 并成为 Kylin 网站。所有 MD 源文件都在 git 中管理，因此可以清楚地跟踪所有更改和贡献者。

## 工作前

在您添加或修改文档前请安装以下工具：  

1. 首先，确保 Ruby 和 Gem 能在您的机器上工作  
	* 对于 Mac 用户，请参考[这个](https://github.com/sstephenson/rbenv#homebrew-on-mac-os-x)来搭建 ruby 环境。
	* 对于 Windows 用户，使用 [ruby installer](http://rubyinstaller.org/downloads/)。
	* 对于 China 用户，考虑使用一个[本地 gem 仓库](https://ruby.taobao.org/)以防止网络问题。

2. 然后，安装 [Jekyll](http://jekyllrb.com)，以及需要的插件
	* `gem install jekyll jekyll-multiple-languages kramdown rouge`  
	* __注意__：一些特定的 jekyll 和 jekyll-multiple-languages 版本不能一起使用（使用 jekyll 3.0.1 和 jekyll-multiple-languages 2.0.3 时我遇到一个 "undefined method" 错误)。这种情况下，`jekyll 2.5.3` 和 `jekyll-multiple-languages 1.0.8` 是已知可运行的版本。
        * 例如. 使用 `gem install jekyll --version "=2.5.3"` 来安装具体的版本。
	* __注意__：对于 Mac 用户，如果 gem 安装时遇到类似这样的错误 'ERROR:  While executing gem ... (Gem::FilePermissionError)'。您可以使用 'brew install ruby' 的方式解决这个问题，然后重启您的终端。
3. 您可以选择任何 markdown 编辑器

下面是一个可以工作的 gem 列表。如果 jekyll 安装成为问题，请坚持使用这些版本。

```
$ gem list

...
jekyll (2.5.3)
jekyll-coffeescript (1.0.1)
jekyll-gist (1.4.0)
jekyll-multiple-languages (1.0.8)
jekyll-paginate (1.1.0)
jekyll-sass-converter (1.4.0)
jekyll-watch (1.3.1)
json (1.8.1)
kramdown (1.9.0)
...
rouge (1.10.1)
...
```

## 使用 Docker 为文档编译

最新版的 kylin 发布提供了 dockerfile，来减少构建复杂性使用 docker 和 Makefile 能调用 docker 命令。

```
$ pwd
/Users/<username>/kylin/website
$ make docker.build
docker build -f Dockerfile -t kylin-document:latest .
Sending build context to Docker daemon  82.44MB
Step 1/3 : FROM jekyll/jekyll:2.5.3
 ---> e81842c29599
Step 2/3 : RUN gem install jekyll-multiple-languages -v 1.0.11
 ---> Using cache
 ---> e9e8b0f1d388
Step 3/3 : RUN gem install rouge -v 3.0.0
 ---> Using cache
 ---> 1bd42c6b93c0
Successfully built 1bd42c6b93c0
Successfully tagged kylin-document:latest
$ make runserver
docker run --volume="/Users/<username>/kylin/website:/srv/jekyll" -p 4000:4000 --rm -it kylin-document:latest jekyll server --watch
Configuration file: /srv/jekyll/_config.yml
            Source: /srv/jekyll
       Destination: /srv/jekyll/_site
      Generating...
...
```

## 关于 Jekyll
Jekyll 是一个用于从源文本和主题生成静态 HTML 网站的 Ruby 脚本，HTML 在部署到 Web 服务器之前生成。Jekyll 恰好也是 GitHub 页面背后的引擎。

Apache Kylin 的网站和文档使用 Jekyll 来管理和生成，可在 [http://kylin.apache.org](http://kylin.apache.org) 上看到最终内容。

## Multi-Language
要草拟中文版文档或翻译现有文档，只需添加或复制该文档，名称以 .cn.md 作为后缀。它将在 /cn 文件夹下生成与 html 同名的文件。
要添加其他语言，请更新 _config.yml 并遵循与中文版相同的模式。  

# Kylin 文档结构以及导航菜单

[作为 Jekyll 源的 Kylin 网站](https://github.com/apache/kylin/tree/document/website)是在 `doucment` 分支下维护的。

1. __Home Page__：_"index.md"_ 文档的主页
2. __Getting Started__：_"gettingstarted"_ 生成 Apache Kylin 的文档，包括 FAQ，术语
3. __Installation__：_"install"_ Apache Kylin 安装指南
4. __Tutorial__：_"tutorial"_ 关于用户如何使用 Apache Kylin 的教程
5. __How To__：_"howto"_ 更细节的帮助指南
6. __Development__：_"development"_ 为了开发者贡献，集成其它应用和扩展 Apache Kylin
7. __Others__：其它文档。

菜单由 Jekyll 集合管理：

* ___data/docs.yml__：英文版本菜单结构  
* ___data/docs-cn.yml__：中文版本菜单结构   
* __add new menu item__：添加新的条目：在相关文件夹下创建新文档，例如 howto_example.md。添加如下的前标记： 

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

将链接更改为完全链接
然后将条目添加到 docs.yml，如：

```
- title: How To
  docs:
  - howto/howto_contribute
  - howto/howto_jdbc
  - howto/howto_example
```

# 如何编写文档
使用任何 markdown 编辑器打开文档，草拟内容并在本地预览。

样例文档：

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

# 如何添加图片
所有的图片请放到 _images_ 文件夹下，在你的文件中，请使用以下样式引入图片：  

```
![](/images/Kylin-Web-Tutorial/2 tables.png)

```

# 如何添加连接
使用站点链接的相对路径，例如

```
[REST API](docs/development/rest_api.html). 

```

# 如何添加代码高亮
我们使用 [Rouge](https://github.com/jneen/rouge) 突出显示代码语法。
查看此 doc 的源代码以获取更多详细信息示例。

# 如何在本地预览
您可以在 markdown 编辑器中预览，要检查网站上的确切内容，请从 `website` 文件夹中运行 Jekyll：

```
jekyll server

```
然后在浏览器中访问 http://127.0.0.1:4000。

## 如何发布到网站（只适用于 committer)  

### 搭建

1. `cd website`
2. `svn co https://svn.apache.org/repos/asf/kylin/site _site`

___site__ 文件夹是工作目录，将由 maven 或 git 随时删除，请确保只有当你想要发布到网站时从 svn 检出。

### 本地运行  
在创建一个 PR 或推送到 git 仓库之前，您可以通过以下方式在本地预览更改：

1. `cd website`
2. `jekyll s`
3. 在您的浏览器打开 [http://127.0.0.1:4000](http://127.0.0.1:4000)

### 推到网站 

1. 拷贝 jekyll 生成的 `_site` 到 svn 的 `website/_site`
2. `cd website/_site`
3. `svn status`
4. 您需要使用 `svn add` 添加任意新的文件
5. `svn commit -m 'UPDATE MESSAGE'`

在几分钟内，svnpubsub 应该开始且您将能够在 [http://kylin.apache.org](http://kylin.apache.org/) 看到结果。


