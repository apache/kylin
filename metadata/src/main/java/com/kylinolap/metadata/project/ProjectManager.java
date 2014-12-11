/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.metadata.project;

import com.google.common.collect.Maps;
import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.persistence.JsonSerializer;
import com.kylinolap.common.persistence.ResourceStore;
import com.kylinolap.common.persistence.Serializer;
import com.kylinolap.common.restclient.Broadcaster;
import com.kylinolap.common.restclient.SingleValueCache;
import com.kylinolap.metadata.model.realization.DataModelRealizationType;
import com.kylinolap.metadata.model.realization.IDataModelRealization;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author xduo
 */
public class ProjectManager {
    private static final Logger logger = LoggerFactory.getLogger(ProjectManager.class);

    // static cached instances
    private static final ConcurrentHashMap<KylinConfig, ProjectManager> CACHE = new ConcurrentHashMap<KylinConfig, ProjectManager>();
    private static final Serializer<ProjectInstance> PROJECT_SERIALIZER = new JsonSerializer<ProjectInstance>(ProjectInstance.class);
    private ConcurrentMap<Class<? extends IDataModelRealization>, Class<? extends IDataModelRealization>> realizations = Maps.newConcurrentMap();

    private KylinConfig config;
    // project name => ProjrectDesc
    private SingleValueCache<String, ProjectInstance> projectMap = new SingleValueCache<String, ProjectInstance>(Broadcaster.TYPE.PROJECT);
    // project name => tables

    public static ProjectManager getInstance(KylinConfig config) {
        ProjectManager r = CACHE.get(config);
        if (r != null) {
            return r;
        }

        synchronized (ProjectManager.class) {
            r = CACHE.get(config);
            if (r != null) {
                return r;
            }
            try {
                r = new ProjectManager(config);
                CACHE.put(config, r);
                if (CACHE.size() > 1) {
                    logger.warn("More than one singleton exist");
                }
                return r;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to init CubeManager from " + config, e);
            }
        }
    }

    public static synchronized void removeInstance(KylinConfig config) {
        CACHE.remove(config);
    }

    private ProjectManager(KylinConfig config) throws IOException {
        logger.info("Initializing CubeManager with metadata url " + config);
        this.config = config;

        loadAllProjects();
    }

    public void registerDataModelRealization(Class<? extends IDataModelRealization> realization) {
        if (realization == null) {
            throw new NullPointerException("realization cannot be null");
        }
        realizations.putIfAbsent(realization, realization);
    }

    public List<ProjectInstance> listAllProjects() {
        return new ArrayList<ProjectInstance>(projectMap.values());
    }

    public List<ProjectInstance> getProjects(String cubeName) {
        return this.findProjects(cubeName);
    }

    public ProjectInstance dropProject(String projectName) throws IOException {
        if (projectName == null)
            throw new IllegalArgumentException("Project name not given");

        ProjectInstance projectInstance = getProject(projectName);

        if (projectInstance == null) {
            throw new IllegalStateException("The project named " + projectName + " does not exist");
        }

        if (projectInstance.getCubes().size() != 0) {
            throw new IllegalStateException("The project named " + projectName + " can not be deleted because there's still cubes in it. Delete all the cubes first.");
        }

        logger.info("Dropping project '" + projectInstance.getName() + "'");
        deleteResource(projectInstance);

        return projectInstance;
    }

    public ProjectInstance getProject(String projectName) {
        if (projectName == null)
            return null;
        projectName = ProjectInstance.getNormalizedProjectName(projectName);
        return projectMap.get(projectName);
    }

    public ProjectInstance createProject(String projectName, String owner, String description) throws IOException {

        logger.info("Creating project '" + projectName);

        ProjectInstance currentProject = getProject(projectName);
        if (currentProject == null) {
            currentProject = ProjectInstance.create(projectName, owner, description, null);
        } else {
            throw new IllegalStateException("The project named " + projectName + "already exists");
        }

        saveResource(currentProject);

        return currentProject;
    }

    public ProjectInstance updateProject(ProjectInstance project, String newName, String newDesc) throws IOException {
        if (!project.getName().equals(newName)) {
            ProjectInstance newProject = this.createProject(newName, project.getOwner(), newDesc);
            newProject.setCreateTime(project.getCreateTime());
            newProject.recordUpdateTime(System.currentTimeMillis());
            newProject.setCubes(project.getCubes());

            deleteResource(project);
            saveResource(newProject);

            return newProject;
        } else {
            project.setName(newName);
            project.setDescription(newDesc);

            if (project.getUuid() == null) {
                project.updateRandomUuid();
            }

            saveResource(project);

            return project;
        }
    }

    public ProjectInstance updateProject(ProjectInstance project) throws IOException{
        saveResource(project);
        return project;
    }

    public void removeCubeFromProjects(String cubeName) throws IOException {
        for (ProjectInstance projectInstance : findProjects(cubeName)) {
            projectInstance.removeCube(cubeName);
            saveResource(projectInstance);
        }
    }

    public ProjectInstance updateCubeToProject(String cubeName, String newProjectName, String owner) throws IOException {
        removeCubeFromProjects(cubeName);
        return addCubeToProject(cubeName, newProjectName, owner);
    }

    public void loadProjectCache(ProjectInstance project, boolean triggerUpdate) throws IOException {
        loadProject(project.getResourcePath(), triggerUpdate);
    }

    public void removeProjectCache(ProjectInstance project) {
        String projectName = ProjectInstance.getNormalizedProjectName(project.getName());
        if (projectMap.containsKey(projectName)) {
            projectMap.remove(projectName);
        }
    }

    private List<ProjectInstance> findProjects(String cubeName) {
        List<ProjectInstance> projects = new ArrayList<ProjectInstance>();
        for (ProjectInstance projectInstance : projectMap.values()) {
            if (projectInstance.containsCube(cubeName)) {
                projects.add(projectInstance);
            }
        }

        return projects;
    }

    private synchronized ProjectInstance loadProject(String path, boolean triggerUpdate) throws IOException {
        ResourceStore store = getStore();
        logger.debug("Loading CubeInstance " + store.getReadableResourcePath(path));

        ProjectInstance projectInstance = store.getResource(path, ProjectInstance.class, PROJECT_SERIALIZER);
        projectInstance.init();

        if (StringUtils.isBlank(projectInstance.getName())) {
            throw new IllegalStateException("Project name must not be blank");
        }

        if (triggerUpdate) {
            projectMap.put(projectInstance.getName().toUpperCase(), projectInstance);
        } else {
            projectMap.putLocal(projectInstance.getName().toUpperCase(), projectInstance);
        }

        return projectInstance;
    }

    private void loadAllProjects() throws IOException {
        ResourceStore store = getStore();
        List<String> paths = store.collectResourceRecursively(ResourceStore.PROJECT_RESOURCE_ROOT, ".json");

        logger.debug("Loading Project from folder " + store.getReadableResourcePath(ResourceStore.PROJECT_RESOURCE_ROOT));

        for (String path : paths) {
            loadProject(path, false);
        }

        logger.debug("Loaded " + paths.size() + " Project(s)");
    }

    private ProjectInstance addCubeToProject(String cubeName, String project, String user) throws IOException {
        String newProjectName = ProjectInstance.getNormalizedProjectName(project);
        ProjectInstance newProject = getProject(newProjectName);
        if (newProject == null) {
            newProject = this.createProject(newProjectName, user, "This is a project automatically added when adding cube " + cubeName);
        }
        newProject.addCube(cubeName);
        saveResource(newProject);

        return newProject;
    }

    private void saveResource(ProjectInstance proj) throws IOException {
        ResourceStore store = getStore();
        store.putResource(proj.getResourcePath(), proj, PROJECT_SERIALIZER);
        afterProjectUpdated(proj);
    }

    private void deleteResource(ProjectInstance proj) throws IOException {
        ResourceStore store = getStore();
        store.deleteResource(proj.getResourcePath());
        this.afterProjectDropped(proj);
    }

    private void afterProjectUpdated(ProjectInstance updatedProject) {
        try {
            this.loadProjectCache(updatedProject, true);
        } catch (IOException e) {
            logger.error(e.getLocalizedMessage(), e);
        }
    }

    private void afterProjectDropped(ProjectInstance droppedProject) {
        this.removeProjectCache(droppedProject);
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

}
