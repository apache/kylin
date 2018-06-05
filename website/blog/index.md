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
              <div class="blog-card">
                <div class="blog-pic">
                  <img width="20" src="../assets/images/icon_blog_w.png">
                </div>
                <p class="blog-title"><a class="post-link" href="{{ post.url | prepend: site.baseurl }}">{{ post.title }}</a></p>
                <p align="left" class="post-meta" >posted: {{ post.date | date: "%b %-d, %Y" }}</p>
              </div>
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
