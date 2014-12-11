KylinApp.service('MessageService', function () {
    var options = {
        extraClasses: 'messenger-fixed messenger-on-top messenger-on-right',
        theme: 'air'
    };

    this.sendMsg = function (msg, type, actions, sticky) {

        var data = {
            message: msg,
            type: angular.isDefined(type) ? type : 'info',
            actions: actions,
            showCloseButton: true
        };

        // Whether sticky the message, otherwise it will hide after a period.
        if (angular.isDefined(sticky) && sticky === true) {
            data.hideAfter = false;
        }

        Messenger(options).post(data);
    }
});
