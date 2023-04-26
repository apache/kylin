---
layout: default-cn
title: 大数据分析型数据仓库
---


<main id="main" >
  <div class="container" >
    <div id="zero" class=" main" >
      <header style=" padding:2em 0 4em 0;">
        <div class="container" >
          <h4 class="index-title"><span>Apache Kylin™ 概览</span></h4>
          <div class="row" style="margin-top:-20px;">
            <div class="col-sm-12 col-md-12">              
              <p class="title_text"> Apache Kylin™是一个开源的、分布式的分析型数据仓库，提供Hadoop/Spark 之上的 SQL 查询接口及多维分析（OLAP）能力以支持超大规模数据，最初由 eBay 开发并贡献至开源社区。它能在亚秒内查询巨大的表。</p>
              <p class="title_text"> Apache Kylin™ 令使用者仅需三步，即可实现超大数据集上的亚秒级查询。</p>
              <div align="left">
                <ol class="none-icon">
                  <li>
                    <span class="li-circle">1</span>
                    定义数据集上的一个星形或雪花形模型
                  </li>
                  <li>
                    <span class="li-circle">2</span>
                    在定义的数据表上构建cube
                  </li>
                  <li>
                    <span class="li-circle">3</span>
                    使用标准 SQL 通过 ODBC、JDBC 或 RESTFUL API 进行查询，仅需亚秒级响应时间即可获得查询结果
                  </li>
                  </ol>
              </div>  
              <p class="title_text">Kylin 提供与多种数据可视化工具的整合能力，如 Tableau，PowerBI 等，令用户可以使用 BI 工具对 Hadoop 数据进行分析。</p>
              <p style="font-weight:bold;color:#87CEFF;text-decoration:underline;text-align:center;"> 👏👏 Kylin 5 已经发布, 请访问全新的 <a href="https://kylin.apache.org/5.0"> Kylin 5.0 官网</a>！ 👈 主要特性:</p>
              <div align="left">
                <ol class="none-icon">
                  <li>
                    <span class="li-circle">1</span>
                    全新的建模 Web 界面，简化用户操作
                  </li>
                  <li>
                    <span class="li-circle">2</span>
                    全新升级的元数据和建模引擎，增加明细索引、可计算列、屏蔽列等特性
                  </li>
                  <li>
                    <span class="li-circle">3</span>
                    即将到来的 Native 向量化计算引擎，可以提升查询性能一倍
                  </li>
                  </ol>
              </div> 
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
        <h4 class="index-title"><span> Kylin 是什么? </span></h4>
        <img id="intro_logo" src="{{"/assets/images/kylin_logo.png" | prepend: site.baseurl }}">
        <!-- second-->
        <div class="row">
          <div class="col-sm-8 col-md-8">
            <div class="col-sm-6 col-md-6 ">
              <div class="card-s">
                <div class="home-pic">
                  <img width="30" src="{{"/assets/images/icon_index_olap.png" | prepend: site.baseurl }}">
                </div>
                <b>可扩展超快的基于大数据的分析型数据仓库: </b><br/>
                <div class="indent" style="margin-top: 25px">Kylin 是为减少在 Hadoop/Spark 上百亿规模数据查询延迟而设计</div>
              </div>
            </div>
            <div class="col-sm-6 col-md-6">
              <div class="card-s">
                <div class="home-pic">
                  <img width="30" src="{{"/assets/images/icon_index_hadoop.png" | prepend: site.baseurl }}">
                </div>
                <b>Hadoop ANSI SQL 接口: </b><br/>
                <div class="indent" style="margin-top: 25px">作为一个分析型数据仓库(也是 OLAP 引擎)，Kylin 为 Hadoop 提供标准 SQL 支持大部分查询功能</div>
              </div>
            </div>
            <div class="col-sm-6 col-md-6">
              <div class="card-s">
                <div class="home-pic">
                  <img width="30" src="{{"/assets/images/icon_index_query.png" | prepend: site.baseurl }}">
                </div>
                <b>交互式查询能力: </b><br/>
                <div class="indent" style="margin-top: 25px">通过 Kylin，用户可以与 Hadoop 数据进行亚秒级交互，在同样的数据集上提供比 Hive 更好的性能</div>
              </div>
            </div>
            <div class="col-sm-6 col-md-6"> 
              <div class="card-s">
                <div class="home-pic">
                  <img width="30" src="{{"/assets/images/icon_index_cube.png" | prepend: site.baseurl }}"> 
                </div>
                <b>多维立方体（MOLAP Cube）: </b><br/>
                <div class="indent" style="margin-top: 25px">用户能够在 Kylin 里为百亿以上数据集定义数据模型并构建立方体</div>
              </div>
            </div>
            <div class="col-sm-6 col-md-6">
              <div class="card-s">
                <div class="home-pic">
                  <img width="30" src="{{"/assets/images/icon_index_streaming.png" | prepend: site.baseurl }}"> 
                </div>
                <b>实时 OLAP：</b><br/>
                <div class="indent">Kylin 可以在数据产生时进行实时处理，用户可以在秒级延迟下进行实时数据的多维分析。 </div>
              </div>
            </div>
            <div class="col-sm-6 col-md-6">
              <div class="card-s">
                <div class="home-pic">
                  <img width="30" src="{{"/assets/images/icon_index_shape.png" | prepend: site.baseurl }}">
                </div>
                <b>与BI工具无缝整合:</b><br/>
                <div class="indent" style="margin-top: 47px">Kylin 提供与 BI 工具的整合能力，如Tableau，PowerBI/Excel，MSTR，QlikSense，Hue 和 SuperSet</div>
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
    <div class="container" >
        <h4 class="index-title" style="margin-top:50px;"><span>谁在使用 Kylin？</span></h4>
        <div class="row" style="margin-top:30px;">
            <!-- 1 -->
            <a class="sponsor" href="http://www.ebay.com/"> 
                <img src="/images/logo/ebay.png">
            </a>
            <a class="sponsor" href="https://www.cisco.com/"> 
                <img src="/images/logo/cisco.jpg">
            </a>
            <a class="sponsor" href="https://about.yahoo.co.jp/info/en/"> 
                <img src="/images/logo/yahoo.png">
            </a>
            <a class="sponsor" href="https://www.samsung.com/cn/"> 
                <img src="/images/logo/samsung.png">
            </a>
            <a class="sponsor" href="http://map.baidu.com/"> 
                <img src="/images/logo/baidu.png">
            </a>
            <!-- 2 -->
            <a class="sponsor" href="https://www.apple.com/"> 
                <img src="/images/logo/apple.jpg">
            </a>
            <a class="sponsor" href="https://www.microsoft.com/"> 
                <img src="/images/logo/microsoft.jpg">
            </a> 
            <a class="sponsor" href="https://www.amazon.com/"> 
                <img src="/images/logo/amazon.png">
            </a>  
            <a class="sponsor" href="https://www.gome.com.cn/"> 
                <img src="/images/logo/gome.png">
            </a>
            <a class="sponsor" href="https://www.jpmorgan.com/"> 
                <img src="/images/logo/jpmorgan.png">
            </a>
            <!-- 3 -->
            <a class="sponsor" href="https://strikingly.com/"> 
                <img style="width: 200px;height: 80px" src="/images/logo/strikingly.png">
            </a>
            <a class="sponsor" href="https://cn.danale.com/"> 
                <img src="/images/logo/danale.png">
            </a>
            <a class="sponsor" href="https://www.58.com/"> 
                <img src="/images/logo/58.png">
            </a>
            <a class="sponsor" href="http://www.jd.com/"> 
                <img src="/images/logo/jd.jpg">
            </a>
            <a class="sponsor" href="http://www.4399.com/"> 
                <img src="/images/logo/4399.png">
            </a>
            <!-- 4 -->
            <a class="sponsor" href="http://www.exponential.com/"> 
                <img src="/images/logo/exponential.jpg">
            </a>
            <a class="sponsor" href="http://www.ctrip.com/"> 
                <img style="width: 200px;height: 80px" src="/images/logo/ctrip.png">
            </a>
            <a class="sponsor" href="http://www.didiglobal.com/"> 
                <img src="/images/logo/didi.png">
            </a>
            <a class="sponsor" href="http://www.dream-it.cn/"> 
                <img src="/images/logo/dreamsoft.png">
            </a>
            <a class="sponsor" href="http://www.meituan.com/"> 
                <img src="/images/logo/meituan.jpg">
            </a>
            <!-- 5 -->
            <a class="sponsor" href="https://kyligence.io/"> 
                <img style="width: 200px;height: 80px;text-align: left;" src="/images/logo/kyligence.png">
            </a>
            <a class="sponsor" href="https://www.envision-group.com/cn/"> 
                <img src="/images/logo/envision.png">
            </a>
            <a class="sponsor" href="https://gameforge.com/"> 
                <img src="/images/logo/gameforge.png">
            </a> 
            <a class="sponsor" href="https://www.glispa.com/"> 
                <img src="/images/logo/glispa.jpg">
            </a>
            <a class="sponsor" href="https://www.sohu.com/"> 
                <img src="/images/logo/soho.png">
            </a>
            <!-- 6 -->
            <a class="sponsor" href="http://www.300.cn/"> 
                <img src="/images/logo/growforce.png">
            </a>
            <a class="sponsor" href="https://www.hobsons.com/"> 
                <img style="width: 200px;height: 80px" src="/images/logo/hobsons.png">
            </a>
            <a class="sponsor" href="http://www.iflytek.com/"> 
                <img src="/images/logo/iflytek.png">
            </a>
            <a class="sponsor" href="http://www.iqiyi.com/"> 
                <img src="/images/logo/iqiyi.png">
            </a>
            <a class="sponsor" href="javascript:void(0);">
                <img src="/images/logo/leeco.png">
            </a>
            <!-- 7 -->
            <a class="sponsor" href="https://www.meizu.com/"> 
                <img src="/images/logo/meizu.png">
            </a>
            <a class="sponsor" href="http://life.pingan.com/"> 
                <img src="/images/logo/pingan.png">
            </a>
            <a class="sponsor" href="https://www.qunar.com/"> 
                <img src="/images/logo/qunar.png">
            </a>
            <a class="sponsor" href="http://www.stratebi.com/"> 
                <img src="/images/logo/stratebi.png">
            </a>
            <a class="sponsor" href="https://www.toutiao.com/"> 
                <img src="/images/logo/toutiao.png">
            </a>
            <!-- 8 -->
            <a class="sponsor" href="https://www.trinitymobility.com/"> 
                <img style="width: 200px;height: 80px" src="/images/logo/trinity.png">
            </a>
            <a class="sponsor" href="http://www.uc.cn/"> 
                <img src="/images/logo/uc.png">
            </a>
            <a class="sponsor" href="http://www.vip.com/"> 
                <img src="/images/logo/vipcom.png">
            </a>
            <a class="sponsor" href="http://www.wanda.cn/"> 
                <img src="/images/logo/wanda.png">
            </a>
            <a class="sponsor" href="http://www.zte.com.cn/"> 
                <img src="/images/logo/zte.png">
            </a>
            <!-- 9 -->             
            <a class="sponsor" href="https://www.infoworks.io/"> 
                <img src="/images/logo/infoworks.png">
            </a>
            <a class="sponsor" href="https://www.expedia.com/"> 
                <img src="/images/logo/expedia.jpg">
            </a>             
            <a class="sponsor" href="https://www.telecoming.com/"> 
                <img src="/images/logo/telecoming.jpg">
            </a>
            <a class="sponsor" href="http://www.163.com/"> 
                <img src="/images/logo/netease.png">
            </a>
            <a class="sponsor" href="http://www.mininglamp.com/"> 
                <img style="width: 200px;height: 80px;margin-bottom: 10px;text-align: left;" src="/images/logo/mininglamp.png">
            </a>
            <!-- 10 -->
            <a class="sponsor" href="https://www.ele.me/home/"> 
                <img src="/images/logo/ele.png">
            </a>
            <a class="sponsor" href="https://www.teld.cn/"> 
                <img src="/images/logo/teld.jpg">
            </a>
            <a class="sponsor" href="https://www.tencent.com/"> 
                <img src="/images/logo/tencent.jpg">
            </a>
            <a class="sponsor" href="http://www.xiaomi.com/"> 
                <img src="/images/logo/xiaomi.jpg">
            </a>
            <a class="sponsor" href="http://www.wanmei.com/">
                <img src="/images/logo/wanmei.jpg">
            </a>
            <!-- 11 -->
            <a class="sponsor" href="http://www.powerbi.com.cn/">
                <img src="/images/logo/aowei.jpg">
            </a>
            <a class="sponsor" href="http://www.chinaoly.com/"> 
                <img src="/images/logo/chinaoly.jpg">
            </a>  
            <a class="sponsor" href="https://www.jianshu.com/">
                <img src="/images/logo/jianshu.jpg">
            </a>
            <a class="sponsor" href="http://www.keruyun.com/"> 
                <img src="/images/logo/keruyun.jpg">
            </a>
            <a class="sponsor" href="https://www.qutoutiao.net/">
                <img src="/images/logo/qutoutiao.jpg">
            </a>
            <!-- 12 -->
            <a class="sponsor" href="http://www.bianfeng.com/">
                <img src="/images/logo/bianfeng.jpg">
            </a>
            <a class="sponsor" href="https://www.missfresh.cn/">
                <img src="/images/logo/meiriyouxian.jpg">
            </a>
            <a class="sponsor" href="https://www.meitu.com/">
                <img src="/images/logo/meitu.jpg">
            </a>
            <a class="sponsor" href="https://www.neusoft.com/"> 
                <img src="/images/logo/neusoft.jpg">
            </a>
            <a class="sponsor" href="https://www.wenjuan.com/">
                <img src="/images/logo/wenjuanwang.jpg">
            </a>
            <!-- 13 -->
            <a class="sponsor" href="http://www.yidianzixun.com/">
                <img src="/images/logo/yidianzixun.jpg">
            </a>
            <a class="sponsor" href="https://www.bestpay.com.cn/">
                <img src="/images/logo/yizhifu.jpg">
            </a>
            <a class="sponsor" href="https://www.zybang.com/">
                <img src="/images/logo/zuoyebang.jpg">
            </a>
            <a class="sponsor" href="http://www.zqykj.com/"> 
                <img src="/images/logo/zhiqiyun.jpg">
            </a>
            <a class="sponsor" href="https://www.epam.com/"> 
                <img src="/images/logo/epam.jpg">
            </a>
            <!-- 14 -->
            <a class="sponsor" href="http://www.moji.com/"> 
                <img src="/images/logo/moji.jpg">
            </a>
            <a class="sponsor" href="http://www.ke.com/"> 
                <img src="/images/logo/beike.jpg">
            </a>
            <a class="sponsor" href="https://www.zto.com/"> 
                <img src="/images/logo/zto.jpg">
            </a>   
            <a class="sponsor" href="https://www.hundsun.com/"> 
                <img src="/images/logo/hundsun.jpg">
            </a>
            <a class="sponsor" href="https://www.deppon.com/"> 
                <img src="/images/logo/deppon.jpg">
            </a> 
            <!-- 15 -->
            <a class="sponsor" href="https://www.dmall.com/"> 
                <img src="/images/logo/dmall.jpg">
            </a>
             <a class="sponsor" href="https://www.youzan.com/"> 
                <img src="/images/logo/youzan.png">
            </a>
        </div>
        <!-- /container --> 
      </div>
   <header>
      <div class="container" >
        <h4 class="index-title"><span>Kylin 生态圈</span></h4>
        <div class="row" style="margin-top:40px;">
          <div class="col-sm-7 col-md-7" id="ecosystem">
            <h6>
              <span class="circle-spot">Kylin 核心:</span>
            </h6> 
            <p>Kylin 基础框架，包括元数据（Metadata）引擎，查询引擎，Job引擎及存储引擎等，同时包括REST服务器以响应客户端请求</p>
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
