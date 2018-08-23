---
layout: community-cn
title: 社区
permalink: /cn/community/index.html
---
<div class="container" >
	<div class="row">
		<div class="col-sm-6 col-md-6">
		    <h2> Apache Kylin 用户案例 </h2>
			<p>想要了解谁在使用 Apache Kylin，请参考 <a href="/cn/community/poweredby.html">powered by</a> 页面。</p>
		</div>

		<div class="col-sm-6 col-md-6">
		    <h2> 社交媒体 </h2>
		    <p>Apache Kylin 官方 Twitter 账号: <a href="https://twitter.com/ApacheKylin">@ApacheKylin</a></p>
		</div>
	</div>

	<div class="row">
		<div class="col-sm-6 col-md-6">
		    <h2> Apache Kylin 邮件群组 </h2>

		    <p>如果您在使用 Kylin 过程中遇到问题，请加入 Kylin 邮件群组参与讨论，这是参与 Apache 项目讨论的正确方式。根据您的参与性质: 普通用户或开发者，可以分别订阅 user@kylin.apache.org 和 dev@kylin.apache.org 邮件群组。</p>

			<p>
		   	请先订阅 Kylin 邮件群组，再发邮件到对应的组，否则您的问题将会被阻止。
			例如，如果您要订阅 user@kylin.apache.org，那么先发一个空邮件到 user-subscribe@kylin.apache.org，稍后您会收到一封确认邮件，对它进行回复即可完成订阅。在订阅成功后，请再发送您的问题到 user@kylin.apache.org，那么所有订阅该邮件组的其它用户将会收到您的邮件，并做出分享。</p>


		    <p>  
		    	<span>用户(user)邮件列表</span>
		    	<span>
		    		<a href="mailto:user-subscribe@kylin.apache.org">订阅</a>
		    	</span>
		    	<span>
		    		<a href="mailto:user-unsubscribe@kylin.apache.org">退订</a>
		    	</span>
		    	<span>
		    	    <a href="mailto:user@kylin.apache.org">发送新邮件</a>
		    	</span>
		    	<span>
		    	    <a href="http://mail-archives.apache.org/mod_mbox/kylin-user/">历史存档</a>
		    	</span>
		    </p>

		    <p>  
		    	<span>开发者(dev)邮件列表</span>
		    	<span>
		    		<a href="mailto:dev-subscribe@kylin.apache.org">订阅</a>
		    	</span>
		    	<span>
		    		<a href="mailto:dev-unsubscribe@kylin.apache.org">退订</a>
		    	</span>
		    	<span>
		    	    <a href="mailto:dev@kylin.apache.org">发送新邮件</a>
		    	</span>
		    	<span>
		    	    <a href="http://mail-archives.apache.org/mod_mbox/kylin-dev//">历史存档</a>
		    	</span>
		    </p>

		    <p>  
		    	<span>Issues 邮件列表</span>
		    	<span>
		    		<a href="mailto:issues-subscribe@kylin.apache.org">订阅</a>
		    	</span>
		    	<span>
		    		<a href="mailto:issues-unsubscribe@kylin.apache.org">退订</a>
		    	</span>
		    	<span>N/A</span>
		    	<span>
		    	    <a href="http://mail-archives.apache.org/mod_mbox/kylin-issues/">历史存档</a>
		    	</span>
		    </p>

		    <p>  
		    	<span>Commits 邮件列表</span>
		    	<span>
		    		<a href="mailto:commits-subscribe@kylin.apache.org">订阅</a>
		    	</span>
		    	<span>
		    		<a href="mailto:commits-unsubscribe@kylin.apache.org">退订</a>
		    	</span>
		        <span>N/A</span>
		    	<span>
		    	    <a href="http://mail-archives.apache.org/mod_mbox/kylin-commits/">历史存档</a>
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
		    <h2>Apache Kylin 开发团队</h2>
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

