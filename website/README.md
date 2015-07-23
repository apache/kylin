<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

# Apache Kylin website  
This directory contains the source code for the Apache Kylin (incubating) website:
[http://kyin.incubator.apache.org](http://kylin.incubator.apache.org/).

## Kylin website's structure:

1. ___config.yml__: Jekyll settings
2. ___layouts__: Page layout template
3. ___includes__: Page template like header, footer...
2. ___data__: Jekyll collections, docs.yml is for Docs menu generation
3. ___docs__: Docuemtation folder
4. ___posts__: Blog folder
5. __download__: Download folder, including released source code package, binary package, ODBC Driver and development version.
6. __cn__: Chinese version 

## Multi-Language
To draft Chinese version document or translate existing one, just add or copy that doc and name with .cn.md as sufffix. It will generate under /cn folder with same name as html file.  
To add other language, please update _config.yml and follow the same pattern as Chinese version.

## Setup

1. `cd website`
2. `svn co https://svn.apache.org/repos/asf/incubator/kylin/site _site`
3. `sudo apt-get install rubygems ruby2.1-dev zlib1g-dev` (linux)
4. `sudo gem install bundler github-pages jekyll`
5. `bundle install`

___site_ folder is working dir which will be removed anytime by maven or git, please make sure only check out from svn when you want to publish to website.

## Running locally  
Before opening a pull request or push to git repo, you can preview changes from your local box with following:

1. `cd website`
2. `jekyll s`
3. Open [http://localhost:4000](http://localhost:4000) in your browser

## Pushing to site (for committer only)  
1. `cd website/_site`
2. `svn status`
3. You'll need to `svn add` any new files
4. `svn commit -m 'UPDATE MESSAGE'`

Within a few minutes, svnpubsub should kick in and you'll be able to
see the results at
[http://kylin.incubator.apache.org](http://kylin.incubator.apache.org/).