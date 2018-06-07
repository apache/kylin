---
layout: blogs
title: Blog
---

<main id="main" >
<section id="first" class="main">
    <header style="padding:2em 0 2em 0;">
      <div class="container" >
        <h4 class="index-title"><span>Apache Kylin™ Technical Blog </span></h4>
         <!-- second-->
          <div id="content-containe" class="animated fadeIn clearfix">
            {% for category in site.categories %}   
            {% if category[0]  == 'blog' %}
            {% for post in category[1] %}
            <div class="col-md-6 col-lg-6 col-xs-6">
              <a class="blog-card" href="{{ post.url | prepend: site.baseurl }}">
                <div class="blog-pic">
                  <img width="20" src="../assets/images/icon_blog_w.png">
                </div>
                <p class="blog-title">{{ post.title }}</p>
                <p align="left" class="post-meta" >posted: {{ post.date | date: "%b %-d, %Y" }}</p>
              </a>
            </div>
      {% endfor %}
      {% endif %}
      {% endfor %}

        </div>

  <p class="rss-subscribe">Subscribe <a href="{{ "/feed.xml" | prepend: site.baseurl }}">via RSS</a></p>
      </div>
      <!-- /container --> 
      
    </header>
  </section>

  
    
</main>
