var module = angular.module('ngLoadingRequest', []);
module.provider('loadingRequest', function () {

    this.$get = ['$document', '$window', function ($document, $window) {
        var body = $document.find('body');

        var loadTemplate = angular.element('<div class="kylinLoadingRequest"><div class="loadingOverlay" ></div>' +
            '<div id="loadingCntnr" class="showbox" style="opacity: 0; margin-top: 250px;">'+
            '<div class="loadingWord" ><img src="/image/waiting.gif"><span>Please wait...</span></div>'+
            '</div> </div>');

        var createOverlay = function () {
                if(!body.find(".kylinLoadingRequest").length){
                   body.append(loadTemplate);
                }
                $(".loadingOverlay").css({'display':'block','opacity':'0.8'});
                $(".showbox").stop(true).animate({'margin-top':'300px','opacity':'1'},200);
            };
        return {
            show: function () {
                createOverlay();
            },
            hide: function () {
                $(".showbox").stop(true).animate({'margin-top':'250px','opacity':'0'},2000);
                $(".loadingOverlay").css({'display':'none','opacity':'0'});
                if(body.find(".kylinLoadingRequest").length){
                    body.find(".kylinLoadingRequest").remove();
                }

            }
        }

    }]
});