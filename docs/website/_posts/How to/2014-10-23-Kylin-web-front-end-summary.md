---
layout: post
title:  "Kylin web front end summary"
date:   2014-10-23
author: xduo
categories: howto
---

### Project Dependencies
* npm: used in development phase to install grunt and bower
* grunt: build and set up kylin web
* bower: manage kylin tech dependencies

### Tech Dependencies
* Angular JS: fundamental support of kylin web
* ACE: sql and json editor
* D3 JS: draw report chart and cube graph
* Bootstrap: css lib

### Supported Use Cases:

###### Kylin web supports needs of various of roles in BI workflow. 

* Analyst: Run query and checkout results
* Modeler: cube design, cube/job operation and monitor
* Admin: system operation.

### Tech Overview 
Kylin web is a one-page application build on top of restful services. Kylin web uses tools from nodejs to manage project and use AngularJS to enable one-page web app. Kylin web uses popular techs from js opensource community making it easy to catch up and contribute. 

### Highlights:
* Query utility functions:
    * SQL auto-suggestions on table and column name
    * Query remote/local save.
    * Data grid supporting million level data with easy BI operations
    * Data export
    * Simple data visualization(line, bar, pie)
* Cube management:
    * Well-designed cube creation flow
    * Visualization of cube relational structure.
    * Well-designed cube access management
* Job management:
    * Job steps and log monitor
    * Kill
    * Resume
* Useful admin tools.
* Refined look&feel.
