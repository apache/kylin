---
layout: default
title: Home
---


<main id="main" >
  <div class="container" >
    <div id="zero" class=" main" >
      <header style=" padding:2em 0 4em 0">
        <div class="container" >

          <h4 class="section-title"><span>Apache Kylin™ Overview</span></h4>
          <div class="row" style="margin-top:-20px;">
            <div class="col-sm-12 col-md-12">              
              <p class="title_text"> Apache Kylin™ is an open source Distributed Analytics Engine designed to provide SQL interface and multi-dimensional analysis (OLAP) on Hadoop supporting extremely large datasets, original contributed from eBay Inc.</p>
              <img id="diagram" src="assets/images/kylin_diagram.png"> </div>
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
        <h4 class="section-title"><span> What is Kylin? </span></h4>
        <!-- second-->
        <div class="row">
          <div class="col-sm-12 col-md-12">
            <div align="left">
              <p> <b>- Extremely Fast OLAP Engine at Scale: </b><br/>
              <div class="indent">Kylin is designed to reduce query latency on Hadoop for 10+ billions of rows of data</div>
              </p>
              <p> <b>- ANSI SQL Interface on Hadoop: </b><br/>
              <div class="indent">Kylin offers ANSI SQL on Hadoop and supports most ANSI SQL query functions</div>
              </p>
              <p> <b>- Interactive Query Capability: </b><br/>
              <div class="indent">Users can interact with Hadoop data via Kylin at sub-second latency, better than Hive queries for the same dataset</div>
              </p>
              <p> <b>- MOLAP Cube:</b><br/>
              <div class="indent">User can define a data model and pre-build in Kylin with more than 10+ billions of raw data records</div>
              </p>
              <p> <b>- Seamless Integration with BI Tools:</b><br/>
              <div class="indent">Kylin currently offers integration capability with BI Tools like Tableau.  Integration with Microstrategy and Excel is coming soon</div>
              </p>
              <p> <b>- Other Highlights:</b> <br/>
              <div class="indent">- Job Management and Monitoring <br/>
                - Compression and Encoding Support <br/>
                - Incremental Refresh of Cubes <br/>
                - Leverage HBase Coprocessor for query latency <br/>
                - Approximate Query Capability for distinct Count (HyperLogLog) <br/>
                - Easy Web interface to manage, build, monitor and query cubes <br/>
                - Security capability to set ACL at Cube/Project Level <br/>
                - Support LDAP Integration </div>
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
        <h4 class="section-title"><span>Kylin Ecosystem</span></h4>
        <div class="row">
          <div class="col-sm-7 col-md-7">
            <p> </p>
            <p><b>Kylin Core:</b> Fundamental framework of Kylin OLAP Engine comprises of Metadata Engine, Query Engine, Job Engine and Storage Engine to run the entire stack. It also includes a REST Server to service client requests</p>
            <p><b>Extensions:</b> Plugins to support additional functions and features </p>
            <p><b>Integration:</b> Lifecycle Management Support to integrate with Job Scheduler,  ETL, Monitoring and Alerting Systems </p>
            <p><b>User Interface:</b> Allows third party users to build customized user-interface atop Kylin core</p>
            <p><b>Drivers:</b> ODBC and JDBC drivers to support different tools and products, such as Tableau</p>
          </div>
          <div class="col-sm-5 col-md-5"> <img id="core" src="assets/images/core.png"> </div>
        </div>
        <!-- /container --> 
      </div>
    </header>
  </section>  
</main>
