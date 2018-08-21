---
layout: community-cn
title: 社区
permalink: /cn/community/index.html
---
<div class="container" >
	<div class="row">
		<div class="col-sm-6 col-md-6">
		    <h2> Apache Kylin™ 技术支持 </h2>
			<p>想要了解更多关于谁使用 Apache Kylin™，请参考 <a href="/cn/community/poweredby.html">技术支持</a> 页面。</p>
		</div>

		<div class="col-sm-6 col-md-6">
		    <h2>社交媒体 </h2>
		    <p>Kylin 官方的 Twitter 账号: <a href="https://twitter.com/ApacheKylin">@ApacheKylin</a></p>
		</div>
	</div>

	<div class="row">
		<div class="col-sm-6 col-md-6">
		    <h2> Apache Kylin 邮件列表 </h2>

		    <p>这是为这个工程建立的邮件列表。对于每一个列表，都有订阅、取消订阅和归档链接。</p>


		    <p>  
		    	<span>用户邮件列表</span>
		    	<span>
		    		<a href="mailto:user-subscribe@kylin.apache.org">订阅</a>
		    	</span>
		    	<span>
		    		<a href="mailto:user-unsubscribe@kylin.apache.org">取消订阅</a>
		    	</span>
		    	<span>
		    	    <a href="mailto:user@kylin.apache.org">提交</a>
		    	</span>
		    	<span>
		    	    <a href="http://mail-archives.apache.org/mod_mbox/kylin-user/">mail-archives.apache.org</a>
		    	</span>
		    </p>

		    <p>  
		    	<span>开发者邮件列表</span>
		    	<span>
		    		<a href="mailto:dev-subscribe@kylin.apache.org">订阅</a>
		    	</span>
		    	<span>
		    		<a href="mailto:dev-unsubscribe@kylin.apache.org">取消订阅</a>
		    	</span>
		    	<span>
		    	    <a href="mailto:dev@kylin.apache.org">提交</a>
		    	</span>
		    	<span>
		    	    <a href="http://mail-archives.apache.org/mod_mbox/kylin-dev//">mail-archives.apache.org</a>
		    	</span>
		    </p>

		    <p>  
		    	<span>Issues 邮件列表</span>
		    	<span>
		    		<a href="mailto:issues-subscribe@kylin.apache.org">订阅</a>
		    	</span>
		    	<span>
		    		<a href="mailto:issues-unsubscribe@kylin.apache.org">取消订阅</a>
		    	</span>
		    	<span>N/A</span>
		    	<span>
		    	    <a href="http://mail-archives.apache.org/mod_mbox/kylin-issues/">mail-archives.apache.org</a>
		    	</span>
		    </p>

		    <p>  
		    	<span>Commits 邮件列表</span>
		    	<span>
		    		<a href="mailto:commits-subscribe@kylin.apache.org">订阅</a>
		    	</span>
		    	<span>
		    		<a href="mailto:commits-unsubscribe@kylin.apache.org">取消订阅</a>
		    	</span>
		        <span>N/A</span>
		    	<span>
		    	    <a href="http://mail-archives.apache.org/mod_mbox/kylin-commits/">mail-archives.apache.org</a>
		    	</span>
		    </p>
		</div>

		<div class="col-sm-6 col-md-6">
		    <h2>社区活动报告</h2>

		    <p><a href="https://github.com/apache/kylin/pulse">Git Pulse</a></p>
		    <p><a href="https://reporter.apache.org/?kylin">Apache 委员会报告</a></p>
		</div>
	</div>

	<div class="row">
		<div class="col-sm-6 col-md-6">
		    <h2> 邮件列表档案 </h2>
		    <p>为方便起见，有一个论坛风格的邮件列表存档，它不是官方 Apache 档案的一部分：</p>

		    <p><a href="http://apache-kylin.74782.x6.nabble.com">Nabble 上的开发者列表档案</a></p>
		</div>

		<div class="col-sm-6 col-md-6">
		    <h2>Apache Kylin 团队</h2>
		    <p>一个成功的项目需要很多人扮演不同的角色。一些成员编写代码，提供项目指导或写文档。其他人作为有价值的测试人员，提交补丁和建议。</p>
		</div>
	</div>
</div>

<div class="kylin-member">
	<div class="container">
		<h2> PMC Members & Committer</h2>
		<div class="clearfix">
		{% for c in site.data.contributors %} 
		  <div class="col-sm-6 col-md-4">
		  	<div class="members-card">
			  	<a href="http://github.com/{{ c.githubId }}"> 
			  		<img class="github-pic" src="{% unless c.avatar %}http://github.com/{{ c.githubId }}.png{% else %}{{ c.avatar }}{% endunless %}">
			  	</a>  
			  	<p class="members-name"> {{ c.name }} </p> 
				<p class="member-role">Org: {{ c.org }} </p>
			  	<p class="members-role">Role : {{ c.role }}</p> 
			  	<p>Apache ID : <a href="http://home.apache.org/phonebook.html?uid={{ c.apacheId }}" class="apache-id">{{ c.apacheId }}</a> </p>  
			</div>
		  </div>
		{% endfor %}
		</div>

        <div class="contributors">
			<p >详细的委员会信息在 <a href="https://projects.apache.org/committee.html?kylin">这里</a>。</p>
			<p >详细的代码贡献在 <a href="https://github.com/apache/kylin/graphs/contributors">这里</a>。</p>

		    <h5>其他的贡献者</h5>
		    <table>
		    <tr>  
		    	<th>姓名</th>
		    	<th>Github</th>
		    	<th></th>
		    </tr>
		    <tr>  
		    	<td>Rui Feng</td>
		    	<td><a href="https://github.com/fengrui129">fengrui129</a></td>
		    	<td>网站设计，Kylin Logo</td>
		    </tr>
		    <tr>  
		    	<td>Luffy Xiao</td>
		    	<td><a href="http://github.com/luffy-xiao">luffy-xiao</a></td>
		    	<td>Kylin Web 应用，REST 服务</td>
		    </tr>
		    <tr>  
		    	<td>Kejia Wang</td>
		    	<td><a href="https://github.com/Kejia-Wang">Kejia-Wang</a></td>
		    	<td>Web 应用, 网站</td>
		    </tr>
		    <tr>  
		    	<td>Yue Yang</td>
		    	<td></td>
		    	<td>Web 应用 UI 设计</td>
		    </tr>
		    </table>
		</div>
	</div>
</div>

<div class="container credits">
  <h2> 荣誉</h2>
  <ul>
  	<li>感谢 <a href="https://www.ebayinc.com/">eBay Inc.</a> 将这个项目贡献到开源社区，首先在 <a href="http://www.ebaytechblog.com/2014/10/20/announcing-kylin-extreme-olap-engine-for-big-data/">eBay Techblog</a>宣布。</li>
  	<li>感谢 <a href="https://www.jetbrains.com/">JetBrains</a> 为我们提供 <a href="https://www.jetbrains.com/idea/">IntelliJ IDEA</a> 的免费 license。</li>
  	<li>感谢 <a href="vikash_agarwal@hotmail.com">Vikash Agarwal</a>，他的文章 <a href="http://www.drdobbs.com/windows/odbc-driver-development/184416434?pgno=5">ODBC Driver Development</a> 和示例代码介绍了如何从头开始编写 ODBC 驱动程序的基本思想。</li>
  </ul>

</div>

