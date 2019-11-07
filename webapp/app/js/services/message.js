/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

KylinApp.service('MessageService', ['config_ui_messenger', function (config_ui_messenger,$log) {

  this.sendMsg = function (msg, type, actions, sticky, position, msgid) {
    var options = {
      'theme': config_ui_messenger.theme
    };

    var data = {
      message: msg,
      type: angular.isDefined(type) ? type : 'info',
      actions: actions,
      showCloseButton: true
    };

    if (angular.isDefined(msgid)) {
      data.id = msgid;
    }

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
