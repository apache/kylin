---
layout: community
title: Community
permalink: /community/index.html
---
<div class="container" >
	<div class="row">
		<div class="col-sm-6 col-md-6">
		    <h2> Powered By Apache Kylin™ </h2>
			<p>For information about who are using Apache Kylin™, please refer to <a href="/community/poweredby.html">Powered By</a> page.</p>
		</div>

		<div class="col-sm-6 col-md-6">
		    <h2>Social Media </h2>
		    <p>The official Kylin Twitter account: <a href="https://twitter.com/ApacheKylin">@ApacheKylin</a></p>
		</div>
	</div>

	<div class="row">
		<div class="col-sm-6 col-md-6">
		    <h2> Apache Kylin Mailing List </h2>

		    <p>These are the mailing lists that have been established for this project. For each list, there is a subscribe, unsubscribe, and an archive link.</p>


		    <p>  
		    	<span>User Mailing List</span>
		    	<span>
		    		<a href="mailto:user-subscribe@kylin.apache.org">Subscribe</a>
		    	</span>
		    	<span>
		    		<a href="mailto:user-unsubscribe@kylin.apache.org">Unsubscribe</a>
		    	</span>
		    	<span>
		    	    <a href="mailto:user@kylin.apache.org">Post</a>
		    	</span>
		    	<span>
		    	    <a href="http://mail-archives.apache.org/mod_mbox/kylin-user/">mail-archives.apache.org</a>
		    	</span>
		    </p>

		    <p>  
		    	<span>Developers Mailing List</span>
		    	<span>
		    		<a href="mailto:dev-subscribe@kylin.apache.org">Subscribe</a>
		    	</span>
		    	<span>
		    		<a href="mailto:dev-unsubscribe@kylin.apache.org">Unsubscribe</a>
		    	</span>
		    	<span>
		    	    <a href="mailto:dev@kylin.apache.org">Post</a>
		    	</span>
		    	<span>
		    	    <a href="http://mail-archives.apache.org/mod_mbox/kylin-dev//">mail-archives.apache.org</a>
		    	</span>
		    </p>

		    <p>  
		    	<span>Issues Mailing List</span>
		    	<span>
		    		<a href="mailto:issues-subscribe@kylin.apache.org">Subscribe</a>
		    	</span>
		    	<span>
		    		<a href="mailto:issues-unsubscribe@kylin.apache.org">Unsubscribe</a>
		    	</span>
		    	<span>N/A</span>
		    	<span>
		    	    <a href="http://mail-archives.apache.org/mod_mbox/kylin-issues/">mail-archives.apache.org</a>
		    	</span>
		    </p>

		    <p>  
		    	<span>Commits Mailing List</span>
		    	<span>
		    		<a href="mailto:commits-subscribe@kylin.apache.org">Subscribe</a>
		    	</span>
		    	<span>
		    		<a href="mailto:commits-unsubscribe@kylin.apache.org">Unsubscribe</a>
		    	</span>
		        <span>N/A</span>
		    	<span>
		    	    <a href="http://mail-archives.apache.org/mod_mbox/kylin-commits/">mail-archives.apache.org</a>
		    	</span>
		    </p>
		</div>

		<div class="col-sm-6 col-md-6">
		    <h2>Community Activity Report</h2>

		    <p><a href="https://github.com/apache/kylin/pulse">Git Pulse</a></p>
		    <p><a href="https://reporter.apache.org/?kylin">Apache Committee Report</a></p>
		</div>
	</div>

	<div class="row">
		<div class="col-sm-6 col-md-6">
		    <h2> Mailing List Archives </h2>
		    <p>For convenience, there's a forum style mailing list archives which not part of offical Apache archives:</p>

		    <p><a href="http://apache-kylin.74782.x6.nabble.com">Developer List archive on Nabble</a></p>
		</div>

		<div class="col-sm-6 col-md-6">
		    <h2>Apache Kylin Team</h2>
		    <p>A successful project requires many people to play many roles. Some members write code, provide project mentorship, or author documentation. Others are valuable as testers, submitting patches and suggestions.</p>
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
			<p >Detailed committee info is <a href="https://projects.apache.org/committee.html?kylin">here</a>.</p>
			<p >Detailed code contribution is <a href="https://github.com/apache/kylin/graphs/contributors">here</a>.</p>

		    <h5>Other contributors</h5>
		    <table>
		    <tr>  
		    	<th>Name</th>
		    	<th>Github</th>
		    	<th></th>
		    </tr>
		    <tr>  
		    	<td>Rui Feng</td>
		    	<td><a href="https://github.com/fengrui129">fengrui129</a></td>
		    	<td>Website Design, Kylin Logo</td>
		    </tr>
		    <tr>  
		    	<td>Luffy Xiao</td>
		    	<td><a href="http://github.com/luffy-xiao">luffy-xiao</a></td>
		    	<td>Kylin Web application, REST service</td>
		    </tr>
		    <tr>  
		    	<td>Kejia Wang</td>
		    	<td><a href="https://github.com/Kejia-Wang">Kejia-Wang</a></td>
		    	<td>Web application, Website</td>
		    </tr>
		    <tr>  
		    	<td>Yue Yang</td>
		    	<td></td>
		    	<td>Web application UI design</td>
		    </tr>
		    </table>
		</div>
	</div>
</div>

<div class="container credits">
  <h2> Credits</h2>
  <ul>
  	<li>Thanks <a href="https://www.ebayinc.com/">eBay Inc.</a> to donated this project to open source community, first announement at <a href="http://www.ebaytechblog.com/2014/10/20/announcing-kylin-extreme-olap-engine-for-big-data/">eBay Techblog</a>. </li>
  	<li>Thanks <a href="https://www.jetbrains.com/">JetBrains</a> for providing us a free license of <a href="https://www.jetbrains.com/idea/">IntelliJ IDEA</a>.</li>
  	<li>Thanks to <a href="vikash_agarwal@hotmail.com">Vikash Agarwal</a>, his article <a href="http://www.drdobbs.com/windows/odbc-driver-development/184416434?pgno=5">ODBC Driver Development</a> and sample code introdued the basic idea about how to write an ODBC driver from scratch.</li>
  </ul>

</div>

