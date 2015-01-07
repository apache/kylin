KylinApp.service('MessageService', ['config_ui_messenger', function (config_ui_messenger) {

    this.sendMsg = function (msg, type, actions, sticky, position) {
        var options = {
            'theme': config_ui_messenger.theme
        };

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

        // Specify the position, otherwise it will be default 'bottom_right'.
        if (angular.isDefined(position) && config_ui_messenger.location.hasOwnProperty(position)) {
            options.extraClasses = config_ui_messenger.location[position];
        }

        Messenger(options).post(data);
    }
}]);

KylinApp.value('config_ui_messenger', {
    location: {
        top_left: 'messenger-fixed messenger-on-top messenger-on-left',
        top_center: 'messenger-fixed messenger-on-top',
        top_right: 'messenger-fixed messenger-on-top message-on-right',
        bottom_left: "messenger-fixed messenger-on-bottom messenger-on-left",
        bottom_center: 'messenger-fixed messenger-on-bottom',
        bottom_right: 'messenger-fixed messenger-on-bottom messenger-on-right'
    },
    theme: 'ice'
});
