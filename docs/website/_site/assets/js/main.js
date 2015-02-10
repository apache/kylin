jQuery(document).ready(function ($) {
	
	
  
	

    //parallax effect
    var parallax_animation = function () {
        $('.parallax').each(function (i, obj) {
            var speed = $(this).attr('parallax-speed');
            if (speed) {
                var background_pos = '-' + (window.pageYOffset / speed) + "px";
                $(this).css('background-position', 'center ' + background_pos);
            }
        });
    }




   // page elements animation 
    var image_animation = function () {
        var diagramTop = $('#diagram').offset().top;
		var coremTop = $('#core').offset().top;

        
        var scroll_top = $(window).scrollTop();
        var currentPosition = scroll_top + 320;

        if (diagramTop < currentPosition) {
            $('#diagram').addClass("animated fadeIn");
        } else {
            $('#diagram').removeClass("animated fadeIn");
        }

        if (coremTop< currentPosition) {

            $('#core').addClass("animated fadeInRight");
        } else {
            $('#core').removeClass("animated fadeInRight");
        }

    }
	
	
	
	
	//document page 
	$( "#content-container" ).load("docs/intro-content.html");
	$( "#left-menu li" ).eq(0).css("background-color", "#efefef");

    $( "#left-menu li" ).click(function(){
		var selectedID = $(this).attr("id");
		$("#content-container").load( "docs/"+selectedID+"-content.html", function() { $(this).fadeIn(500);});
		$(this).css("background-color", "#efefef");
		$(this).siblings().css("background-color", "transparent")
		});
 
  $('#nav-wrapper').height($("#nav").height());
    
    $('#nav').affix({
        offset: { top: $('#nav').offset().top }
    });
	



    $(document).scroll(function () {
        
        parallax_animation();
		image_animation();
       

    });



});