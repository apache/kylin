---
layout: default-cn
title: 首页
---


<main id="main" >
  <div class="container" >
    <div id="zero" class=" main" >
      <header style=" padding:2em 0 4em 0">
        <div class="container" >

          <h4 class="section-title"><span>Apache Kylin 概览</span></h4>
          <div class="row" style="margin-top:-20px;">
            <div class="col-sm-12 col-md-12">
              <p class="title_text"> Kylin 于2014年11月25日被接受会Apache孵化器项目</p>
              <p class="title_text"> Apache Kylin一个开源的分布式分析引擎，提供Hadoop之上的SQL查询接口及多维分析（OLAP）能力以支持超大规模数据，最初由eBay Inc. 开发并贡献至开源社区。</p>
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
        <h4 class="section-title"><span> Kylin是什么? </span></h4>
        <!-- second-->
        <div class="row">
          <div class="col-sm-12 col-md-12">
            <div align="left">
              <p> <b>- 可扩展超快OLAP引擎: </b><br/>
              <div class="indent">Kylin是为减少在Hadoop上百亿规模数据查询延迟而设计</div>
              </p>
              <p> <b>- Hadoop ANSI SQL 接口: </b><br/>
              <div class="indent">Kylin为Hadoop提供标准SQL支持大部分查询功能</div>
              </p>
              <p> <b>- 交互式查询能力: </b><br/>
              <div class="indent">通过Kylin，用户可以与Hadoop数据进行亚秒级交互，在同样的数据集上提供比Hive更好的性能</div>
              </p>
              <p> <b>- 多维立方体（MOLAP Cube）:</b><br/>
              <div class="indent">用户能够在Kylin里为百亿以上数据集定义数据模型并构建立方体</div>
              </p>
              <p> <b>- 与BI工具无缝整合:</b><br/>
              <div class="indent">Kylin提供与BI工具，如Tableau，的整合能力，即将提供对其他工具的整合</div>
              </p>
              <p> <b>- 其他特性:</b> <br/>
              <div class="indent">- Job管理与监控 <br/>
                - 压缩与编码 <br/>
                - 增量更新 <br/>
                - 利用HBase Coprocessor<br/>
                - 基于HyperLogLog的Dinstinc Count近似算法 <br/>
                - 友好的web界面以管理，监控和使用立方体 <br/>
                - 项目及立方体级别的访问控制安全<br/>
                - 支持LDAP </div>
              </p>
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
        <h4 class="section-title"><span>Kylin 生态圈</span></h4>
        <div class="row">
          <div class="col-sm-7 col-md-7">
            <p> </p>
            <p><b>Kylin 核心:</b> Kylin OLAP引擎基础框架，包括元数据（Metadata）引擎，查询引擎，Job引擎及存储引擎等，同时包括REST服务器以响应客户端请求</p>
            <p><b>扩展:</b> 支持额外功能和特性的插件</p>
            <p><b>整合:</b> 与调度系统，ETL，监控等生命周期管理系统的整合</p>
            <p><b>用户界面:</b> 在Kylin核心之上扩展的第三方用户界面</p>
            <p><b>驱动:</b> ODBC 和 JDBC 驱动以支持不同的工具和产品，比如Tableau</p>
          </div>
          <div class="col-sm-5 col-md-5">    </div>
        </div>
        <!-- /container --> 
      </div>
    </header>
  </section>  
</main>
