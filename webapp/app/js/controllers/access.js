'use strict';

KylinApp.controller('AccessCtrl', function ($scope,AccessService, MessageService, AuthenticationService,sweet) {

    $scope.accessTooltip = "<div style='text-align: left'>" +
        "<label>What does access mean to cube?</label>" +
        "<ul><li>CUBE QUERY: Access to query cube</li>" +
        "<li>CUBE OPERATION: Access to rebuild, resume and cancel jobs. Also include access of CUBE QUERY.</li>" +
        "<li>CUBE MANAGEMENT: Access to edit/delete cube. Also include access of CUBE OPERATION.</li>" +
        "<li>CUBE ADMIN: Full access to cube and jobs, including access management.</li></ul></div>";

    $scope.authorities = null;

    AuthenticationService.authorities({}, function (authorities) {
        $scope.authorities = authorities.stringList;
    });

    $scope.resetNewAcess = function () {
        $scope.newAccess = null;
    }

    $scope.renewAccess = function (entity) {
        $scope.newAccess = {
            uuid: entity.uuid,
            sid: null,
            principal: true,
            permission: 'READ'
        };
    }

    $scope.grant = function (type, entity, grantRequst) {
        var uuid = grantRequst.uuid;
        delete grantRequst.uuid;
        AccessService.grant({type: type, uuid: uuid}, grantRequst, function (accessEntities) {
            entity.accessEntities = accessEntities;
            $scope.resetNewAcess();
//            MessageService.sendMsg('Access granted!', 'success', {});
            sweet.show('Success!', 'Access granted!', 'success');
        }, function (e) {
            if (e.status == 404) {
//                MessageService.sendMsg('User not found!', 'error', {});
                sweet.show('Oops...', 'User not found!!', 'error');
            }
        });
    }

    $scope.update = function (type, entity, access, permission) {
        var updateRequst = {
            accessEntryId: access.id,
            permission: permission
        };
        AccessService.update({type: type, uuid: entity.uuid}, updateRequst, function (accessEntities) {
            entity.accessEntities = accessEntities;
//            MessageService.sendMsg('Access granted!', 'success', {});
            sweet.show('', 'Access granted!', 'success');
        });

    }

    $scope.revoke = function (type, access, entity) {
        sweet.show({
            title: 'Confirm',
            text: 'Are you sure to revoke the access?',
            type: 'warning',
            showCancelButton: true,
            confirmButtonColor: '#DD6B55',
            confirmButtonText: "Yes",
            closeOnConfirm: false
        }, function() {
            var revokeRequst = {
                type: type,
                uuid: entity.uuid,
                accessEntryId: access.id
            };
            AccessService.revoke(revokeRequst, function (accessEntities) {
                entity.accessEntities = accessEntities.accessEntryResponseList;
                sweet.show('Success!', 'The access has been revoked.', 'success');
            });

        });

    }
});

