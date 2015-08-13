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

KylinApp.service('ProjectModel', function () {

  this.projects = [];
  this.selectedProject = "_null";


  this.setSelectedProject = function (project) {
    if (this.projects.indexOf(project) > -1||!project) {
      this.selectedProject = project;
    }
  };
  this.getSelectedProject = function (project) {
    return this.selectedProject;
  };

  this.setProjects = function (projects) {
    if (projects.length) {
      this.projects = projects;
    }
  }

  this.addProject = function (project) {
    this.projects.push(project);
    this.sortProjects();
  }

  this.removeProject = function (project) {
    var index = this.projects.indexOf(project);
    if (index > -1) {
      this.projects.splice(index, 1);
    }
    this.selectedProject = this.projects[0];
    this.sortProjects();
  }

  this.updateProject = function (_new, _old) {
    var index = this.projects.indexOf(_old);
    if (index > -1) {
      this.projects[index] = _new;
    }
  }

  this.getProjects = function () {
    return this.projects;
  }

  this.sortProjects = function () {
    this.projects = _.sortBy(this.projects, function (i) {
      return i.toLowerCase();
    });
  }

})
