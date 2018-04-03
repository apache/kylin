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

package org.apache.kylin.metadata.project;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.realization.RealizationType;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Project is a concept in Kylin similar to schema in DBMS
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class ProjectInstance extends RootPersistentEntity implements ISourceAware {

    public static final String DEFAULT_PROJECT_NAME = "default";

    public static ProjectInstance create(String name, String owner, String description, LinkedHashMap<String, String> overrideProps, List<RealizationEntry> realizationEntries, List<String> models) {
        ProjectInstance projectInstance = new ProjectInstance();

        projectInstance.updateRandomUuid();
        projectInstance.setName(name);
        projectInstance.setOwner(owner);
        projectInstance.setDescription(description);
        projectInstance.setStatus(ProjectStatusEnum.ENABLED);
        projectInstance.setCreateTimeUTC(System.currentTimeMillis());
        projectInstance.setOverrideKylinProps(overrideProps);

        if (realizationEntries != null)
            projectInstance.setRealizationEntries(realizationEntries);
        else
            projectInstance.setRealizationEntries(Lists.<RealizationEntry> newArrayList());
        if (models != null)
            projectInstance.setModels(models);
        else
            projectInstance.setModels(new ArrayList<String>());
        return projectInstance;
    }

    // ============================================================================

    private KylinConfigExt config;

    @JsonProperty("name")
    private String name;

    @JsonProperty("tables")
    private Set<String> tables = new TreeSet<String>();

    @JsonProperty("owner")
    private String owner;

    @JsonProperty("status")
    private ProjectStatusEnum status;

    @JsonProperty("create_time_utc")
    private long createTimeUTC;

    @JsonProperty("last_update_time")
    // FIXME why not RootPersistentEntity.lastModified??
    private String lastUpdateTime;

    @JsonProperty("description")
    private String description;

    @JsonProperty("realizations")
    private List<RealizationEntry> realizationEntries;

    @JsonProperty("models")
    private List<String> models;

    @JsonProperty("ext_filters")
    private Set<String> extFilters = new TreeSet<String>();

    @JsonProperty("override_kylin_properties")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private LinkedHashMap<String, String> overrideKylinProps;

    public void init() {
        if (name == null)
            name = ProjectInstance.DEFAULT_PROJECT_NAME;

        if (realizationEntries == null) {
            realizationEntries = new ArrayList<RealizationEntry>();
        }

        if (tables == null)
            tables = new TreeSet<String>();

        if (overrideKylinProps == null) {
            overrideKylinProps = new LinkedHashMap<>();
        }

        initConfig();

        if (StringUtils.isBlank(this.name))
            throw new IllegalStateException("Project name must not be blank");
    }

    private void initConfig() {
        this.config = KylinConfigExt.createInstance(KylinConfig.getInstanceFromEnv(), this.overrideKylinProps);
    }

    public String getResourcePath() {
        return concatResourcePath(resourceName());
    }

    public static String concatResourcePath(String projectName) {
        return ResourceStore.PROJECT_RESOURCE_ROOT + "/" + projectName + ".json";
    }

    @Override
    public String resourceName() {
        return this.name;
    }
    
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setExtFilters(Set<String> extFilters) {
        this.extFilters = extFilters;
    }

    public ProjectStatusEnum getStatus() {
        return status;
    }

    public void setStatus(ProjectStatusEnum status) {
        this.status = status;
    }

    public long getCreateTimeUTC() {
        return createTimeUTC;
    }

    public void setCreateTimeUTC(long createTimeUTC) {
        this.createTimeUTC = createTimeUTC;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean containsRealization(final RealizationType type, final String realization) {
        return Iterables.any(this.realizationEntries, new Predicate<RealizationEntry>() {
            @Override
            public boolean apply(RealizationEntry input) {
                return input.getType() == type && input.getRealization().equalsIgnoreCase(realization);
            }
        });
    }

    public void removeRealization(final RealizationType type, final String realization) {
        Iterables.removeIf(this.realizationEntries, new Predicate<RealizationEntry>() {
            @Override
            public boolean apply(RealizationEntry input) {
                return input.getType() == type && input.getRealization().equalsIgnoreCase(realization);
            }
        });
    }

    public List<RealizationEntry> getRealizationEntries(final RealizationType type) {
        if (type == null)
            return getRealizationEntries();

        return ImmutableList.copyOf(Iterables.filter(realizationEntries, new Predicate<RealizationEntry>() {
            @Override
            public boolean apply(@Nullable RealizationEntry input) {
                return input.getType() == type;
            }
        }));
    }

    public int getRealizationCount(final RealizationType type) {

        if (type == null)
            return this.realizationEntries.size();

        return Iterables.size(Iterables.filter(this.realizationEntries, new Predicate<RealizationEntry>() {
            @Override
            public boolean apply(RealizationEntry input) {
                return input.getType() == type;
            }
        }));
    }

    public void addRealizationEntry(final RealizationType type, final String realizationName) {
        RealizationEntry pdm = new RealizationEntry();
        pdm.setType(type);
        pdm.setRealization(realizationName);
        this.realizationEntries.add(pdm);
    }

    public void setTables(Set<String> tables) {
        this.tables = tables;
    }

    public boolean containsTable(String tableName) {
        return tables.contains(tableName.toUpperCase());
    }

    public void removeTable(String tableName) {
        tables.remove(tableName.toUpperCase());
    }

    public void addExtFilter(String extFilterName) {
        this.getExtFilters().add(extFilterName);
    }

    public void removeExtFilter(String filterName) {
        extFilters.remove(filterName);
    }

    public void addTable(String tableName) {
        tables.add(tableName.toUpperCase());
    }

    public Set<String> getTables() {
        return tables;
    }

    public Set<String> getExtFilters() {
        return extFilters;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public void recordUpdateTime(long timeMillis) {
        this.lastUpdateTime = formatTime(timeMillis);
    }

    public List<RealizationEntry> getRealizationEntries() {
        return realizationEntries;
    }

    public void setRealizationEntries(List<RealizationEntry> entries) {
        this.realizationEntries = entries;
    }

    public List<String> getModels() {
        return models;
    }

    public boolean containsModel(String modelName) {
        return models != null && models.contains(modelName);
    }

    public void setModels(List<String> models) {
        this.models = models;
    }

    public void addModel(String modelName) {
        if (this.getModels() == null) {
            this.setModels(new ArrayList<String>());
        }
        this.getModels().add(modelName);
    }

    public void removeModel(String modelName) {
        if (this.getModels() != null) {
            this.getModels().remove(modelName);
        }
    }

    public LinkedHashMap<String, String> getOverrideKylinProps() {
        return overrideKylinProps;
    }

    void setOverrideKylinProps(LinkedHashMap<String, String> overrideKylinProps) {
        if (overrideKylinProps == null) {
            overrideKylinProps = new LinkedHashMap<>();
        }
        this.overrideKylinProps = overrideKylinProps;
        initConfig();
    }

    public KylinConfig getConfig() {
        return config;
    }

    public void setConfig(KylinConfigExt config) {
        this.config = config;
    }

    @Override
    public String toString() {
        return "ProjectDesc [name=" + name + "]";
    }

    @Override
    public int getSourceType() {
        return getConfig().getDefaultSource();
    }
}
