$(function() {
	var myNav1 = $("#nside1 .list-group"),i,j;
	for(i=0;i<myNav1.length;i++) {
		var myid = myNav1.eq(i).attr("id");
		var myNav2 = $("#" + myid + " a");
		for(j=0;j<myNav2.length;j++) {
			var links = myNav2.eq(j).attr("href"), myURL = document.URL;
			if(myURL.indexOf(links) != -1) {
				myNav2.eq(j).attr('class','nside');
			}
		}
	}
})
