KylinApp.service('ProjectModel',function(){

    this.projects = [];
    this.selectedProject =null;


    this.setSelectedProject = function(project) {
        if(this.projects.indexOf(project) > -1) {
            this.selectedProject = project;
        }
    };

    this.setProjects = function(projects){
        if(projects.length){
            this.projects = projects;
        }
    }

    this.addProject = function(project){
        this.projects.push(project);
        this.sortProjects();
    }

    this.removeProject = function(project){
        var index =this.projects.indexOf(project);
        if(index>-1){
            this.projects.splice(index,1);
        }
    }

    this.updateProject = function (_new,_old) {
        var index =this.projects.indexOf(_old);
        if(index>-1){
            this.projects[index] = _new;
        }
    }

    this.getProjects = function(){
        return this.projects;
    }

    this.sortProjects = function (){
        this.projects = _.sortBy(this.projects, function (i) { return i.toLowerCase(); });
    }

})