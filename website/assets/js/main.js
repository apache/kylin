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
        var diagramTop = $('#diagram').offset() ? $('#diagram').offset().top : 0;
		var coremTop = $('#core').offset() ? $('#core').offset().top : 0;
        var logoTop = $('#intro_logo').offset() ? $('#intro_logo').offset().top : 0;
        
        var scroll_top = $(window).scrollTop();
        var currentPosition = scroll_top + 320;

        

        if (coremTop > 0 && coremTop< currentPosition) {

            $('#core').addClass("animated fadeInRight");
        } else {
            $('#core').removeClass("animated fadeInRight");
        }
        if (diagramTop > 0 && diagramTop < currentPosition) {

            $('#diagram').addClass("animated fadeInRight");
        } else {
            $('#diagram').removeClass("animated fadeInRight");
        }
        if (logoTop > 0 && logoTop< currentPosition) {

            //$('#intro_logo').addClass("animated fadeInRight");
        } else {
            $('#intro_logo').removeClass("animated fadeInRight");
        }
    }
	
	
	
	
	//document page 
	$( "#content-container" ).load("docs/What-should-I-use-Kylin-for.md");
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