---
layout: dev
title:  How to Build Binary Package
categories: development
permalink: /development/howto_package.html
---

### Generate Binary Package
{% highlight bash %}
git clone https://github.com/apache/incubator-kylin kylin
cd kylin
./script/package.sh
{% endhighlight %}

In order to generate binary package, **maven** and **npm** are pre-requisites.