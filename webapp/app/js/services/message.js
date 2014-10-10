KylinApp.service('MessageService', function (GraphBuilder) {
    this.sendMsg = function ($msg, $type, $actions) {
        Messenger().post({
            message: $msg,
            type: angular.isDefined($type) ? $type : 'info',
            actions: $actions,
            showCloseButton: true
        });
    }
});