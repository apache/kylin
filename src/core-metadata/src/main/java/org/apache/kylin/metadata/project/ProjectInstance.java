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
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.AutoMergeTimeEnum;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.MaintainModelType;
import org.apache.kylin.metadata.model.RetentionRange;
import org.apache.kylin.metadata.model.SegmentConfig;
import org.apache.kylin.metadata.model.VolatileRange;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import lombok.Getter;
import lombok.Setter;
import lombok.val;

/**
 * Project is a concept in Kylin similar to schema in DBMS
 */
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class ProjectInstance extends RootPersistentEntity implements ISourceAware {

    public static final String DEFAULT_PROJECT_NAME = "default";

    public static final String DEFAULT_DATABASE = "DEFAULT";

    public static final String EXPOSE_COMPUTED_COLUMN_CONF = "kylin.query.metadata.expose-computed-column";

    private KylinConfigExt config;

    @JsonProperty("name")
    private String name;

    @JsonProperty("owner")
    private String owner;

    @JsonProperty("status")
    private ProjectStatusEnum status;

    @JsonProperty("create_time_utc")
    private long createTimeUTC;

    @JsonProperty("default_database")
    @Getter
    @Setter
    private String defaultDatabase;

    @JsonProperty("description")
    private String description;

    @JsonProperty("principal")
    @Getter
    @Setter
    private String principal;

    @JsonProperty("keytab")
    @Getter
    @Setter
    private String keytab;

    @JsonProperty("maintain_model_type")
    @Getter
    private MaintainModelType maintainModelType = MaintainModelType.MANUAL_MAINTAIN;

    @JsonProperty("override_kylin_properties")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private LinkedHashMap<String, String> overrideKylinProps;

    @JsonProperty("segment_config")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Getter
    @Setter
    private SegmentConfig segmentConfig = new SegmentConfig(false, Lists.newArrayList(AutoMergeTimeEnum.WEEK,
            AutoMergeTimeEnum.MONTH, AutoMergeTimeEnum.QUARTER, AutoMergeTimeEnum.YEAR), new VolatileRange(),
            new RetentionRange(), false);

    @Override
    public String getResourcePath() {
        return concatResourcePath(resourceName());
    }

    public static String concatResourcePath(String projectName) {
        return ResourceStore.PROJECT_ROOT + "/" + projectName + MetadataConstants.FILE_SURFIX;
    }

    public static ProjectInstance create(String name, String owner, String description,
            LinkedHashMap<String, String> overrideProps) {
        ProjectInstance projectInstance = new ProjectInstance();

        projectInstance.setName(name);
        projectInstance.setOwner(owner);
        projectInstance.setDescription(description);
        projectInstance.setStatus(ProjectStatusEnum.ENABLED);
        projectInstance.setCreateTimeUTC(System.currentTimeMillis());
        projectInstance.setDefaultDatabase(ProjectInstance.DEFAULT_DATABASE);
        projectInstance.setOverrideKylinProps(overrideProps);
        return projectInstance;
    }

    public void initConfig(KylinConfig config) {
        // https://olapio.atlassian.net/browse/KE-11535
        // to compatible with existing model
        // if expose-computed-column is empty, set it with the maintainModelType
        if (!overrideKylinProps.containsKey(EXPOSE_COMPUTED_COLUMN_CONF)) {
            overrideKylinProps.put(EXPOSE_COMPUTED_COLUMN_CONF, KylinConfig.TRUE);
        }
        this.config = KylinConfigExt.createInstance(config, filterNonCustomConfigs(this.overrideKylinProps));
    }

    // ============================================================================

    public ProjectInstance() {
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

    public boolean isSemiAutoMode() {
        return getConfig().isSemiAutoMode();
    }

    public boolean isExpertMode() {
        return !getConfig().isSemiAutoMode();
    }

    public boolean isProjectKerberosEnabled() {
        return config.getKerberosProjectLevelEnable() && StringUtils.isNotBlank(principal)
                && StringUtils.isNotBlank(keytab);
    }

    public ImmutableList<RealizationEntry> getRealizationEntries() {
        return ImmutableList.copyOf(getRealizationsFromResource(name));
    }

    public ImmutableList<String> getModels() {
        return ImmutableList.copyOf(getModelsFromResource(name));
    }

    public ImmutableSet<String> getTables() {
        return ImmutableSet.copyOf(getTableFromResource(name));
    }

    public int getRealizationCount(final String realizationType) {
        val realizationEntries = getRealizationsFromResource(this.name);
        if (realizationType == null)
            return realizationEntries.size();

        return (int) realizationEntries.stream().filter(input -> input.getType().equals(realizationType)).count();
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public boolean containsModel(String modelId) {
        List<String> models = getModelsFromResource(name);
        return models.contains(modelId);
    }

    public LinkedHashMap<String, String> getOverrideKylinProps() {
        return overrideKylinProps;
    }

    public Map<String, String> getLegalOverrideKylinProps() {
        return filterNonCustomConfigs(overrideKylinProps);
    }

    public void putOverrideKylinProps(String key, String value) {
        overrideKylinProps.put(StringUtils.trim(key), StringUtils.trim(value));
    }

    public void replaceKeyOverrideKylinProps(String originKey, String destKey) {
        String value = overrideKylinProps.get(originKey);
        if (StringUtils.isNotEmpty(value)) {
            overrideKylinProps.remove(originKey);
            putOverrideKylinProps(destKey, value);
        }
    }

    public void setOverrideKylinProps(LinkedHashMap<String, String> overrideKylinProps) {

        overrideKylinProps = KylinConfig.trimKVFromMap(overrideKylinProps);
        this.overrideKylinProps = overrideKylinProps;
        if (config != null) {
            this.config = KylinConfigExt.createInstance(config.base(), filterNonCustomConfigs(overrideKylinProps));
        }
    }

    private LinkedHashMap<String, String> filterNonCustomConfigs(LinkedHashMap<String, String> overrideKylinProps) {
        val nonCustomConfigs = KylinConfig.getInstanceFromEnv().getUserDefinedNonCustomProjectConfigs();
        val filteredOverrideKylinProps = new LinkedHashMap<String, String>();
        overrideKylinProps.entrySet().stream().filter(entry -> !nonCustomConfigs.contains(entry.getKey()))
                .forEach(entry -> filteredOverrideKylinProps.put(entry.getKey(), entry.getValue()));
        return filteredOverrideKylinProps;
    }

    @Override
    public KylinConfigExt getConfig() {
        return config;
    }

    public void setConfig(KylinConfigExt config) {
        this.config = config;
    }

    public void init(KylinConfig config) {
        init(config, false);
    }

    public void init(KylinConfig config, boolean copyOverrides) {
        if (name == null)
            name = ProjectInstance.DEFAULT_PROJECT_NAME;

        if (copyOverrides && config instanceof KylinConfigExt) {
            overrideKylinProps = (LinkedHashMap<String, String>) ((KylinConfigExt) config).getExtendedOverrides();
        }

        if (overrideKylinProps == null) {
            overrideKylinProps = new LinkedHashMap<>();
        }

        initConfig(config);

        if (StringUtils.isBlank(this.name))
            throw new IllegalStateException("Project name must not be blank");
    }

    @Override
    public String toString() {
        return "ProjectDesc [name=" + name + "]";
    }

    @Override
    public int getSourceType() {
        return getConfig().getDefaultSource();
    }

    private List<String> getModelsFromResource(String projectName) {
        String modeldescRootPath = getProjectRootPath(projectName) + ResourceStore.DATA_MODEL_DESC_RESOURCE_ROOT;
        Set<String> modelResource = getStore().listResources(modeldescRootPath);
        return getNameListFromResource(modelResource);
    }

    private String getProjectRootPath(String prj) {
        return "/" + prj;
    }

    private List<RealizationEntry> getRealizationsFromResource(String projectName) {
        String dataflowRootPath = getProjectRootPath(projectName) + ResourceStore.DATAFLOW_RESOURCE_ROOT;
        Set<String> realizationResource = getStore().listResources(dataflowRootPath);

        if (realizationResource == null)
            return new ArrayList<>();

        List<String> realizations = getNameListFromResource(realizationResource);
        List<RealizationEntry> realizationEntries = new ArrayList<>();
        for (String realization : realizations) {
            RealizationEntry entry = RealizationEntry.create("NCUBE", realization);
            realizationEntries.add(entry);
        }

        return realizationEntries;
    }

    private Set<String> getTableFromResource(String projectName) {
        String tableRootPath = getProjectRootPath(projectName) + ResourceStore.TABLE_RESOURCE_ROOT;
        Set<String> tableResource = getStore().listResources(tableRootPath);
        if (tableResource == null)
            return new TreeSet<>();
        List<String> tables = getNameListFromResource(tableResource);
        return new TreeSet<>(tables);
    }

    //drop the path ahead name and drop suffix e.g [/default/model_desc/]nmodel_basic[.json]
    private List<String> getNameListFromResource(Set<String> modelResource) {
        if (modelResource == null)
            return new ArrayList<>();
        List<String> nameList = new ArrayList<>();
        for (String resource : modelResource) {
            String[] path = resource.split("/");
            resource = path[path.length - 1];
            resource = StringUtil.dropSuffix(resource, MetadataConstants.FILE_SURFIX);
            nameList.add(resource);
        }
        return nameList;
    }

    ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

}
