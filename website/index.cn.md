---
layout: default-cn
title: 首页
---


<main id="main" >
  <div class="container" >
    <div id="zero" class=" main" >
      <header style=" padding:2em 0 4em 0;">
        <div class="container" >
          <h4 class="index-title"><span>Apache Kylin™ 概览</span></h4>
          <div class="row" style="margin-top:-20px;">
            <div class="col-sm-12 col-md-12">              
              <p class="title_text"> Apache Kylin™是一个开源的分布式分析引擎，提供Hadoop/Spark之上的SQL查询接口及多维分析（OLAP）能力以支持超大规模数据，最初由eBay Inc. 开发并贡献至开源社区。它能在亚秒内查询巨大的Hive表。</p>
              <img id="diagram" src="{{ "/assets/images/kylin_diagram.png"| prepend: site.baseurl }}"> </div>
          </div>
        </div>
        <!-- /container --> 
      </header>
    </div>
    <!-- / section --> 
  </div>
  <!-- /container -->
  <section id="second" class="main">
    <header style="background-color:#efefef;">
      <div class="container"  >
        <h4 class="index-title"><span> Kylin是什么? </span></h4>
        <img id="intro_logo" src="{{"/assets/images/kylin_logo.png" | prepend: site.baseurl }}">
        <!-- second-->
        <div class="row">
          <div class="col-sm-8 col-md-8">
            <div class="col-sm-6 col-md-6 ">
              <div class="card-s">
                <div class="home-pic">
                  <img width="30" src="{{"/assets/images/icon_index_olap.png" | prepend: site.baseurl }}">
                </div>
                <b>可扩展超快OLAP引擎: </b><br/>
                <div class="indent" style="margin-top: 25px">Kylin是为减少在Hadoop/Spark上百亿规模数据查询延迟而设计</div>
              </div>
            </div>
            <div class="col-sm-6 col-md-6">
              <div class="card-s">
                <div class="home-pic">
                  <img width="30" src="{{"/assets/images/icon_index_hadoop.png" | prepend: site.baseurl }}">
                </div>
                <b>Hadoop ANSI SQL 接口: </b><br/>
                <div class="indent" style="margin-top: 25px">Kylin为Hadoop提供标准SQL支持大部分查询功能</div>
              </div>
            </div>
            <div class="col-sm-6 col-md-6">
              <div class="card-s">
                <div class="home-pic">
                  <img width="30" src="{{"/assets/images/icon_index_query.png" | prepend: site.baseurl }}">
                </div>
                <b>交互式查询能力: </b><br/>
                <div class="indent" style="margin-top: 25px">通过Kylin，用户可以与Hadoop数据进行亚秒级交互，在同样的数据集上提供比Hive更好的性能</div>
              </div>
            </div>
            <div class="col-sm-6 col-md-6"> 
              <div class="card-s">
                <div class="home-pic">
                  <img width="30" src="{{"/assets/images/icon_index_cube.png" | prepend: site.baseurl }}"> 
                </div>
                <b>多维立方体（MOLAP Cube）: </b><br/>
                <div class="indent" style="margin-top: 25px">用户能够在Kylin里为百亿以上数据集定义数据模型并构建立方体</div>
              </div>
            </div>
            <div class="col-sm-12 col-md-12">
              <div class="card-s">
                <div class="home-pic">
                  <img width="30" src="{{"/assets/images/icon_index_shape.png" | prepend: site.baseurl }}">
                </div>
                <b>与BI工具无缝整合:</b><br/>
                <div class="indent" style="margin-top: 47px">Kylin提供与BI工具的整合能力，如Tableau，PowerBI/Excel，MSTR，QlikSense，Hue和SuperSet</div>
              </div>
            </div>
          </div>
          <div class="col-sm-4 col-md-4 card-l">
            <b>其他特性:</b> <br/>
            <ul class="indent">
              <li>Job管理与监控 </li>
              <li>压缩与编码 </li>
              <li>增量更新 </li>
              <li>利用HBase Coprocessor</li>
              <li>基于HyperLogLog的Dinstinc Count近似算法</li>
              <li>友好的web界面以管理，监控和使用立方体 </li>
              <li>项目及表级别的访问控制安全</li>
              <li>支持LDAP、SSO </li>
            </ul>
            <div class="other-pic">
              <img width="90" src="{{"/assets/images/icon_index_highlights.png" | prepend: site.baseurl }}">
            </div>
          </div>
        </div>
      </div>
      <!-- /container --> 
    </header>
  </section>
  <!-- second -->
  <section id="first" class="main">
   <header>
      <div class="container" >
        <h4 class="index-title"><span>Kylin 生态圈</span></h4>
        <div class="row" style="margin-top:40px;">
          <div class="col-sm-7 col-md-7" id="ecosystem">
            <h6>
              <span class="circle-spot">Kylin 核心:</span>
            </h6> 
            <p>Kylin OLAP引擎基础框架，包括元数据（Metadata）引擎，查询引擎，Job引擎及存储引擎等，同时包括REST服务器以响应客户端请求</p>
            <h6>
              <span class="circle-spot">扩展:</span>
            </h6> 
            <p>支持额外功能和特性的插件</p>
            <h6>
              <span class="circle-spot">整合:</span>
            </h6> 
            <p>与调度系统，ETL，监控等生命周期管理系统的整合</p>
            <h6>
              <span class="circle-spot">用户界面:</span>
            </h6> 
            <p>在Kylin核心之上扩展的第三方用户界面</p>
            <h6>
              <span class="circle-spot">驱动:</span>
            </h6> 
            <p>ODBC 和 JDBC 驱动以支持不同的工具和产品，比如Tableau</p>
          </div>
          <div class="col-sm-5 col-md-5"> <img id="core" src="{{"/assets/images/core.png"| prepend: site.baseurl }}"> </div>
        </div>
        <!-- /container --> 
      </div>
    </header>
  </section>  
</main>
