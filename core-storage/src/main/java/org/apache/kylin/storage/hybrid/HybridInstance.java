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
package org.apache.kylin.storage.hybrid;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationRegistry;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.shaded.com.google.common.collect.Lists;

/**
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class HybridInstance extends RootPersistentEntity implements IRealization {

    private final static Logger logger = LoggerFactory.getLogger(HybridInstance.class);

    public static HybridInstance create(KylinConfig config, String name, List<RealizationEntry> realizationEntries) {
        HybridInstance hybridInstance = new HybridInstance();

        hybridInstance.setConfig(config);
        hybridInstance.setName(name);
        hybridInstance.setRealizationEntries(realizationEntries);
        hybridInstance.updateRandomUuid();

        return hybridInstance;
    }

    // ============================================================================

    @JsonIgnore
    private KylinConfig config;

    @JsonProperty("name")
    private String name;

    @JsonProperty("realizations")
    private List<RealizationEntry> realizationEntries;

    @JsonProperty("cost")
    private int cost = 50;

    private volatile IRealization[] realizations = null;
    private List<TblColRef> allDimensions = null;
    private Set<TblColRef> allColumns = null;
    private Set<ColumnDesc> allColumnDescs = null;
    private List<MeasureDesc> allMeasures = null;
    private long dateRangeStart;
    private long dateRangeEnd;
    private boolean isReady = false;

    @Override
    public String resourceName() {
        return name;
    }

    public List<RealizationEntry> getRealizationEntries() {
        return realizationEntries;
    }

    public void setRealizationEntries(List<RealizationEntry> realizationEntries) {
        this.realizationEntries = realizationEntries;
    }

    private void init() {
        if (realizations != null)
            return;

        synchronized (this) {
            if (realizations != null)
                return;

            if (realizationEntries == null || realizationEntries.size() == 0)
                return;

            RealizationRegistry registry = RealizationRegistry.getInstance(config);
            List<IRealization> realizationList = Lists.newArrayList();
            for (int i = 0; i < realizationEntries.size(); i++) {
                IRealization realization = registry.getRealization(realizationEntries.get(i).getType(), realizationEntries.get(i).getRealization());
                if (realization == null) {
                    logger.error("Realization '" + realizationEntries.get(i) + " is not found, remove from Hybrid '" + this.getName() + "'");
                    continue;
                }
                if (realization.isReady() == false) {
                    logger.error("Realization '" + realization.getName() + " is disabled, remove from Hybrid '" + this.getName() + "'");
                    continue;
                }
                realizationList.add(realization);
            }

            LinkedHashSet<TblColRef> columns = new LinkedHashSet<TblColRef>();
            LinkedHashSet<TblColRef> dimensions = new LinkedHashSet<TblColRef>();
            LinkedHashSet<MeasureDesc> measures = new LinkedHashSet<MeasureDesc>();
            dateRangeStart = 0;
            dateRangeEnd = Long.MAX_VALUE;
            for (IRealization realization : realizationList) {
                columns.addAll(realization.getAllColumns());
                dimensions.addAll(realization.getAllDimensions());
                measures.addAll(realization.getMeasures());

                if (realization.isReady())
                    isReady = true;

                if (dateRangeStart == 0 || realization.getDateRangeStart() < dateRangeStart)
                    dateRangeStart = realization.getDateRangeStart();

                if (dateRangeStart == Long.MAX_VALUE || realization.getDateRangeEnd() > dateRangeEnd)
                    dateRangeEnd = realization.getDateRangeEnd();
            }

            allDimensions = Lists.newArrayList(dimensions);
            allColumns = columns;
            allColumnDescs = asColumnDescs(allColumns);
            allMeasures = Lists.newArrayList(measures);

            Collections.sort(realizationList, new Comparator<IRealization>() {
                @Override
                public int compare(IRealization o1, IRealization o2) {

                    long i1 = o1.getDateRangeStart();
                    long i2 = o2.getDateRangeStart();
                    long comp = i1 - i2;
                    if (comp != 0) {
                        return comp > 0 ? 1 : -1;
                    }

                    i1 = o1.getDateRangeEnd();
                    i2 = o2.getDateRangeEnd();
                    comp = i1 - i2;
                    if (comp != 0) {
                        return comp > 0 ? 1 : -1;
                    }

                    return 0;
                }
            });

            this.realizations = realizationList.toArray(new IRealization[realizationList.size()]);
        }
    }

    private Set<ColumnDesc> asColumnDescs(Set<TblColRef> columns) {
        LinkedHashSet<ColumnDesc> result = new LinkedHashSet<>();
        for (TblColRef col : columns) {
            result.add(col.getColumnDesc());
        }
        return result;
    }

    @Override
    public CapabilityResult isCapable(SQLDigest digest) {
        CapabilityResult result = new CapabilityResult();
        result.cost = Integer.MAX_VALUE;

        for (IRealization realization : getRealizations()) {
            CapabilityResult child = realization.isCapable(digest);
            if (child.capable) {
                result.capable = true;
                result.cost = Math.min(result.cost, child.cost);
                result.influences.addAll(child.influences);
            } else {
                result.incapableCause = child.incapableCause;
            }
        }

        if (result.cost > 0)
            result.cost--; // let hybrid win its children

        return result;
    }

    @Override
    public int getCost() {
        int c = Integer.MAX_VALUE;
        for (IRealization realization : getRealizations()) {
            c = Math.min(realization.getCost(), c);
        }
        return c;
    }

    @Override
    public RealizationType getType() {
        return RealizationType.HYBRID;
    }

    @Override
    public DataModelDesc getModel() {
        if (this.getLatestRealization() != null) {
            return this.getLatestRealization().getModel();
        }
        // all included cubes are disabled
        if (this.getRealizationEntries() != null && this.getRealizationEntries().size() > 0) {
            String cubeName = this.getRealizationEntries().get(0).getRealization();
            CubeInstance cubeInstance = CubeManager.getInstance(config).getCube(cubeName);
            return cubeInstance.getModel();
        }
        return null;
    }

    @Override
    public Set<TblColRef> getAllColumns() {
        init();
        return allColumns;
    }

    @Override
    public Set<ColumnDesc> getAllColumnDescs() {
        init();
        return allColumnDescs;
    }

    @Override
    public List<MeasureDesc> getMeasures() {
        init();
        return allMeasures;
    }

    @Override
    public boolean isReady() {
        return isReady;
    }

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return getCanonicalName();
    }

    @Override
    public String getCanonicalName() {
        return getType() + "[name=" + name + "]";
    }

    @Override
    public KylinConfig getConfig() {
        return config;
    }

    public void setConfig(KylinConfig config) {
        this.config = config;
    }

    @Override
    public long getDateRangeStart() {
        return dateRangeStart;
    }

    @Override
    public long getDateRangeEnd() {
        return dateRangeEnd;
    }

    @Override
    public boolean supportsLimitPushDown() {
        return false;
    }

    @Override
    public List<TblColRef> getAllDimensions() {
        init();
        return allDimensions;
    }

    public IRealization[] getRealizations() {
        init();
        return realizations == null ? new IRealization[0] : realizations;
    }

    public String getResourcePath() {
        return concatResourcePath(name);
    }

    public static String concatResourcePath(String hybridName) {
        return ResourceStore.HYBRID_RESOURCE_ROOT + "/" + hybridName + ".json";
    }

    public void setCost(int cost) {
        this.cost = cost;
    }

    public IRealization getLatestRealization() {
        if (getRealizations().length > 0) {
            return realizations[realizations.length - 1];
        }
        return null;
    }

    @Override
    public int getStorageType() {
        return ID_HYBRID;
    }
}
