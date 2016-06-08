---
layout: dev
title:  How to Build Binary Package
categories: development
permalink: /development/howto_package.html
---

### Generate Binary Package
{% highlight bash %}
git clone https://github.com/apache/kylin kylin
cd kylin
./build/script/package.sh
{% endhighlight %}

In order to generate binary package, **maven** and **npm** are pre-requisites.

If you're behind a proxy server, both npm and bower need be told with the proxy info before running ./script/package.sh:

{% highlight bash %}
export http_proxy=http://your-proxy-host:port
npm config set proxy http://your-proxy-host:port
{% endhighlight %}