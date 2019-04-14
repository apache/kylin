---
layout: docs24
title:  "Superset"
categories: tutorial
permalink: /docs24/tutorial/superset.html
---
### Integrate Apache Kylin and Apache Superset

##### Introduction
Apache Superset (incubating) is a modern, enterprise-ready business intelligence web application. The entire backend of Superset is based on Python and uses Flask, Pandas, and SqlAlchemy. It can be integrated with the Kylin Python Client.

##### Features of Apache Superset
* A rich set of data visualizations
* An easy-to-use interface for exploring and visualizing data
* Create and share dashboards
* Enterprise-ready authentication with integration with major authentication providers (database, OpenID, LDAP, OAuth & REMOTE_USER through Flask AppBuilder)
* An extensible, high-granularity security / permission model allowing intricate rules on who can access individual features and the dataset
* A simple semantic layer, allowing users to control how data sources are displayed in the UI by defining which fields should show up in which drop-down and which aggregation and function metrics are made available to the user
* Integration with most SQL-speaking RDBMS through SQLAlchemy

##### Benefits of integration
Both Apache Kylin and Apache Superset are built to provide fast and interactive analytics for their users. The combination of these two open source projects can bring that goal to reality on petabyte-scale datasets, thanks to pre-calculated Kylin Cube.

##### Steps of integration
1. Install Apache Kylin
2. Build Cube successfully
3. Install Apache Superset and initialize it
4. Connect Apache Kylin from ApacheSuperset
5. Configure a new data source
6. Test and query

Please read [this article](http://kylin.apache.org/blog/2018/01/01/kylin-and-superset/) for detailed steps.

##### Other functionalities
Apache Superset also supports exporting to CSV, sharing, and viewing SQL query.

