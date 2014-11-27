'use strict';

KylinApp
    .controller('ProjectCtrl', function ($scope, $modal, $q, ProjectService, MessageService) {
        $scope.projects = [];
        $scope.loading = false;
        $scope.theaditems = [
            {attr: 'name', name: 'Name'},
            {attr: 'owner', name: 'Owner'},
            {attr: 'description', name: 'Description'},
            {attr: 'create_time', name: 'Create Time'}
        ];

        $scope.state = { filterAttr: 'name', filterReverse: true, reverseColumn: 'name'};

        $scope.list = function (offset, limit) {
            offset = (!!offset) ? offset : 0;
            limit = (!!limit) ? limit : 20;
            var defer = $q.defer();
            var queryParam = {offset: offset, limit: limit};

            $scope.loading = true;
            ProjectService.list(queryParam, function (projects) {
                $scope.projects = $scope.projects.concat(projects);
                angular.forEach(projects, function (project) {
                    $scope.listAccess(project, 'ProjectInstance');
                });
                $scope.loading = false;
                defer.resolve(projects.length);
            });

            return defer.promise;
        }

        $scope.toEdit = function(project) {
            $modal.open({
                templateUrl: 'project.html',
                controller: projCtrl,
                resolve: {
                    projects: function () {
                        return $scope.projects;
                    },
                    project: function(){
                        return project;
                    }
                }
            });
        }

        $scope.delete = function(project){
            if (confirm("Are you sure to delete "))
            {
                ProjectService.delete({projecId: project.name}, function(){
                    var pIndex = $scope.projects.indexOf(project);
                    if (pIndex > -1) {
                        $scope.projects.splice(pIndex, 1);
                    }
                    MessageService.sendMsg("Project " + project.name + " deleted successfully!", 'success');
                });
            }
        }
    }
);

