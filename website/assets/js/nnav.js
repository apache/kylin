$(function() {
	var myNav = $("#bs-example-navbar-collapse-1 a"),i;
	for(i=0;i<myNav.length;i++) {
		var links = myNav.eq(i).attr("href"), myURL = document.URL;
		if (links != "/" & links != "/cn") {
			if(myURL.indexOf(links) != -1) {
				myNav.eq(i).attr('class','nnav');
				myNav.eq(0).attr('class','nnav-home');
				break;
			}
		}
		else if (links == "/" | links == "/cn")  {
			myNav.eq(0).attr('class','nnav');
		}
	}
})
