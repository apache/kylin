---
layout: default
title: Home
---


<main id="main" >
  <div class="container" >
    <div id="zero" class=" main" >
      <header style=" padding:2em 0 4em 0;">
        <div class="container" >
          <h4 class="index-title"><span>Apache Kylin™ Overview</span></h4>
          <div class="row" style="margin-top:-20px;">
            <div class="col-sm-12 col-md-12">              
              <p class="title_text">Apache Kylin™ is an open source Distributed Analytics Engine designed to provide SQL interface and multi-dimensional analysis (OLAP) on Hadoop/Spark supporting extremely large datasets, original contributed from eBay Inc.</p>
              <p class="title_text">Apache Kylin™ lets you query massive data set at sub-second latency in 3 steps.</p>
              <div align="left">
                <ol class="none-icon">
                  <li>
                    <span class="li-circle">1</span>
                    Identify a Star/Snowflake Schema on Hadoop.
                  </li>
                  <li>
                    <span class="li-circle">2</span>
                    Build Cube from the identified tables.
                  </li>
                  <li>
                    <span class="li-circle">3</span>
                    Query with ANSI-SQL and get results in sub-second, via ODBC, JDBC or RESTful API.
                  </li>
                </ol>
              </div>
              <img id="diagram" src="assets/images/kylin_diagram.png">
            </div>
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
      <div class="container">
        <h4 class="index-title"><span> What is Kylin? </span></h4>
        <img id="intro_logo" src="assets/images/kylin_logo.png">
        <!-- second-->
        <div class="row">
          <div class="col-sm-8 col-md-8">
            <div class="col-sm-6 col-md-6 ">
              <div class="card-s">
                <div class="home-pic">
                  <img width="30" src="assets/images/icon_index_olap.png">
                </div>
                <b>Extremely Fast OLAP Engine at Scale: </b><br/>
                <div class="indent" style="margin-top: 25px">Kylin is designed to reduce query latency on Hadoop/Spark for 10+ billions of rows of data</div>
              </div>
            </div>
            <div class="col-sm-6 col-md-6">
              <div class="card-s">
                <div class="home-pic">
                  <img width="30" src="assets/images/icon_index_hadoop.png">
                </div>
                <b>ANSI SQL Interface on Hadoop: </b><br/>
                <div class="indent" style="margin-top: 25px">Kylin offers ANSI SQL on Hadoop/Spark and supports most ANSI SQL query functions</div>
              </div>
            </div>
            <div class="col-sm-6 col-md-6">
              <div class="card-s">
                <div class="home-pic">
                  <img width="30" src="assets/images/icon_index_query.png">
                </div>
                <b>Interactive Query Capability: </b><br/>
                <div class="indent" style="margin-top: 25px">Users can interact with Hadoop data via Kylin at sub-second latency, better than Hive queries for the same dataset</div>
              </div>
            </div>
            <div class="col-sm-6 col-md-6"> 
              <div class="card-s">
                <div class="home-pic">
                  <img width="30" src="assets/images/icon_index_cube.png"> 
                </div>
                <b>MOLAP Cube:</b><br/>
                <div class="indent" style="margin-top: 25px">User can define a data model and pre-build in Kylin with more than 10+ billions of raw data records</div>
              </div>
            </div>
            <div class="col-sm-12 col-md-12">
              <div class="card-s">
                <div class="home-pic">
                  <img width="30" src="assets/images/icon_index_shape.png">
                </div>
                <b>Seamless Integration with BI Tools:</b><br/>
                <div class="indent">Kylin currently offers integration capability with BI Tools like Tableau, PowerBI/Excel, MSTR, QlikSense, Hue and SuperSet. </div>
              </div>
            </div>
          </div>
          <div class="col-sm-4 col-md-4 card-l">
            <b>Other Highlights:</b> <br/>
            <ul class="indent">
              <li>Job Management and Monitoring </li>
              <li>Compression and Encoding Support </li>
              <li>Incremental Refresh of Cubes </li>
              <li>Leverage HBase Coprocessor for query latency </li>
              <li>Both approximate and precise Query Capabilities for Distinct Count</li>
              <li>Approximate Top-N Query Capability</li>
              <li>Easy Web interface to manage, build, monitor and query cubes </li>
              <li>Security capability to set ACL at Project/Table Level </li>
              <li>Support LDAP and SAML Integration </li>
            </ul>
            <div class="other-pic">
              <img width="90" src="assets/images/icon_index_highlights.png">
            </div>
          </div>
        </div>
      </div>
      <!-- /container --> 
    </header>
  </section>
  <!-- second -->
  <section id="first" class="main">
      <div class="container" >
        <h4 class="index-title" style="margin-top:50px;"><span>Who are using Kylin?</span></h4>
        <div class="row" style="margin-top:30px;">
            <!-- 1 -->
            <a href="https://www.gome.com.cn/"> 
                <img style="width: 200px;height: 80px;margin-bottom: 10px" src="/images/logo/gome.png">
            </a>
            <a href="https://www.toutiao.com/"> 
                <img style="width: 200px;height: 80px;margin-bottom: 10px" src="/images/logo/toutiao.png">
            </a>
            <a href="http://map.baidu.com/"> 
                <img style="width: 200px;height: 80px;margin-bottom: 10px" src="/images/logo/baidu.png">
            </a>
            <a href="http://www.jd.com/"> 
                <img style="width: 200px;height: 80px;margin-bottom: 10px" src="/images/logo/jd.png">
            </a>
            <a href="https://about.yahoo.co.jp/info/en/"> 
                <img style="width: 200px;height: 80px;margin-bottom: 10px" src="/images/logo/yahoo.png">
            </a>
            <!-- 2 -->
            <a href="http://www.4399.com/"> 
                <img style="width: 200px;height: 80px;margin-bottom: 10px" src="/images/logo/4399.png">
            </a>
            <a href="http://www.didiglobal.com/"> 
                <img style="width: 200px;height: 80px;margin-bottom: 10px" src="/images/logo/didi.png">
            </a>
            <a href="http://www.uc.cn/"> 
                <img style="width: 200px;height: 80px;margin-bottom: 10px" src="/images/logo/uc.png">
            </a>
            <a href="https://www.cisco.com/"> 
                <img style="width: 200px;height: 80px;margin-bottom: 10px" src="/images/logo/cisco.png">
            </a>
            <a href="https://www.infoworks.io/"> 
                <img style="width: 200px;height: 80px;margin-bottom: 10px" src="/images/logo/infoworks.png">
            </a>
            <!-- 3 -->
            <a href="http://life.pingan.com/"> 
                <img style="width: 200px;height: 80px;margin-bottom: 10px" src="/images/logo/pingan.png">
            </a>
            <a href="http://www.iqiyi.com/"> 
                <img style="width: 200px;height: 80px;margin-bottom: 10px" src="/images/logo/iqiyi.png">
            </a>
            <a href="http://www.mininglamp.com/"> 
                <img style="width: 200px;height: 80px;margin-bottom: 10px" src="/images/logo/mininglamp.png">
            </a>
            <a href="http://www.vip.com/"> 
                <img style="width: 200px;height: 80px;margin-bottom: 10px" src="/images/logo/vipcom.png">
            </a>
            <a href="http://www.163.com/"> 
                <img style="width: 200px;height: 80px;margin-bottom: 10px" src="/images/logo/netease.png">
            </a>
            <!-- 4 -->
            <a href="http://www.stratebi.com/"> 
                <img style="width: 200px;height: 80px;margin-bottom: 10px" src="/images/logo/stratebi.png">
            </a>
            <a href="https://www.qunar.com/"> 
                <img style="width: 200px;height: 80px;margin-bottom: 10px" src="/images/logo/qunar.png">
            </a>
            <a href="http://www.ebay.com/"> 
                <img style="width: 200px;height: 80px;margin-bottom: 10px" src="/images/logo/ebay.png">
            </a>
            <a href="https://www.58.com/"> 
                <img style="width: 200px;height: 80px;margin-bottom: 10px" src="/images/logo/58.png">
            </a>
            <img style="width: 200px;height: 80px;margin-bottom: 10px" src="/images/logo/leeco.png">
            <!-- 5 -->
            <a href="https://www.samsung.com/cn/"> 
                <img style="width: 200px;height: 80px;margin-bottom: 10px" src="/images/logo/samsung.png">
            </a>
            <a href="https://gameforge.com/"> 
                <img style="width: 200px;height: 80px;margin-bottom: 10px" src="/images/logo/gameforge.png">
            </a>
            <a href="http://www.300.cn/"> 
                <img style="width: 200px;height: 80px;margin-bottom: 10px" src="/images/logo/growforce.png">
            </a>
            <a href="http://www.wanda.cn/"> 
                <img style="width: 200px;height: 80px;margin-bottom: 10px" src="/images/logo/wanda.png">
            </a>
            <a href="http://www.dream-it.cn/"> 
                <img style="width: 200px;height: 80px;margin-bottom: 10px" src="/images/logo/dreamsoft.png">
            </a>
            <!-- 6 -->
            <a href="http://www.iflytek.com/"> 
                <img style="width: 200px;height: 80px;margin-bottom: 10px" src="/images/logo/iflytek.png">
            </a>
            <a href="https://cn.danale.com/"> 
                <img style="width: 200px;height: 80px;margin-bottom: 10px" src="/images/logo/danale.png">
            </a>
            <a href="http://www.meituan.com/"> 
                <img style="width: 200px;height: 80px;margin-bottom: 10px" src="/images/logo/meituan.png">
            </a>
            <a href="https://www.envision-group.com/cn/"> 
                <img style="width: 200px;height: 80px;margin-bottom: 10px" src="/images/logo/envision.png">
            </a>
            <a href="https://www.meizu.com/"> 
                <img style="width: 200px;height: 80px;margin-bottom: 10px" src="/images/logo/meizu.png">
            </a>
            <!-- 7 -->
            <a href="https://www.sohu.com/"> 
                <img style="width: 200px;height: 80px;margin-bottom: 10px" src="/images/logo/soho.png">
            </a>
            <a href="https://www.jpmorgan.com/"> 
                <img style="width: 200px;height: 80px;margin-bottom: 10px" src="/images/logo/jpmorgan.png">
            </a>
            <a href="https://www.glispa.com/"> 
                <img style="width: 200px;height: 80px;margin-bottom: 10px" src="/images/logo/glispa.png">
            </a>
            <a href="http://www.exponential.com/"> 
                <img style="width: 200px;height: 80px;margin-bottom: 10px" src="/images/logo/exponential.png">
            </a>
            <a href="http://www.zte.com.cn/"> 
                <img style="width: 200px;height: 80px;margin-bottom: 10px" src="/images/logo/zte.png">
            </a>
            <!-- 8 -->
            <a href="https://www.trinitymobility.com/"> 
                <img style="width: 200px;height: 80px" src="/images/logo/trinity.png">
            </a>
            <a href="https://www.hobsons.com/"> 
                <img style="width: 200px;height: 80px" src="/images/logo/hobsons.png">
            </a>
            <a href="https://strikingly.com/"> 
                <img style="width: 200px;height: 80px" src="/images/logo/strikingly.png">
            </a>
            <a href="http://www.ctrip.com/"> 
                <img style="width: 200px;height: 80px" src="/images/logo/ctrip.png">
            </a>
            <a href="https://www.ele.me/home/"> 
                <img style="width: 200px;height: 80px" src="/images/logo/ele.png">
            </a>
        </div>
        <!-- /container --> 
      </div>
    <header>
      <div class="container" >
        <h4 class="index-title"><span>Kylin Ecosystem</span></h4>
        <div class="row" style="margin-top:40px;">
          <div class="col-sm-7 col-md-7" id="ecosystem">
            <h6>
              <span class="circle-spot">Kylin Core:</span>
            </h6> 
            <p>Fundamental framework of Kylin OLAP Engine comprises of Metadata Engine, Query Engine, Job Engine and Storage Engine to run the entire stack. It also includes a REST Server to service client requests</p>
            <h6>
              <span class="circle-spot">Extensions:</span>
            </h6> 
            <p>Plugins to support additional functions and features </p>
            <h6>
              <span class="circle-spot">Integration:</span>
            </h6> 
            <p>Lifecycle Management Support to integrate with Job Scheduler,  ETL, Monitoring and Alerting Systems </p>
            <h6>
              <span class="circle-spot">User Interface:</span>
            </h6> 
            <p>Allows third party users to build customized user-interface atop Kylin core</p>
            <h6>
              <span class="circle-spot">Drivers:</span>
            </h6> 
            <p>ODBC and JDBC drivers to support different tools and products, such as Tableau</p>
          </div>
          <div class="col-sm-5 col-md-5"> <img id="core" src="assets/images/core.png"> </div>
        </div>
        <!-- /container --> 
      </div>
    </header>
  </section>  
</main>
