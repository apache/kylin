package org.apache.kylin.storage.hybrid;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationRegistry;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.metadata.realization.SQLDigest;

import java.util.LinkedHashSet;
import java.util.List;

/**
 */

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class HybridInstance extends RootPersistentEntity implements IRealization {

    @JsonIgnore
    private KylinConfig config;

    @JsonProperty("name")
    private String name;

    @JsonProperty("realizations")
    private List<RealizationEntry> realizationEntries;

    @JsonProperty("cost")
    private int cost = 50;

    private IRealization[] realizations = null;
    private List<TblColRef> allDimensions = null;
    private List<TblColRef> allColumns = null;
    private List<MeasureDesc> allMeasures = null;
    private long dateRangeStart;
    private long dateRangeEnd;
    private boolean isReady = false;
    private String projectName;

    private boolean initiated = false;

    public List<RealizationEntry> getRealizationEntries() {
        return realizationEntries;
    }

    private void init() {
        if (initiated == true)
            return;

        synchronized (this) {
            if (initiated == true)
                return;

            if (realizationEntries == null || realizationEntries.size() == 0)
                throw new IllegalArgumentException();

            RealizationRegistry registry = RealizationRegistry.getInstance(config);
            realizations = new IRealization[realizationEntries.size()];
            for (int i = 0; i < realizationEntries.size(); i++) {
                IRealization realization = registry.getRealization(realizationEntries.get(i).getType(), realizationEntries.get(i).getRealization());
                if (realization == null)
                    throw new IllegalArgumentException("Realization '" + realizationEntries.get(i) + "' not loaded.");
                realizations[i] = realization;
            }

            LinkedHashSet<TblColRef> columns = new LinkedHashSet<TblColRef>();
            LinkedHashSet<TblColRef> dimensions = new LinkedHashSet<TblColRef>();
            LinkedHashSet<MeasureDesc> measures = new LinkedHashSet<MeasureDesc>();
            dateRangeStart = 0;
            dateRangeEnd = Long.MAX_VALUE;
            for (IRealization realization : realizations) {
                if (realization.isReady() == false)
                    continue;

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
            allColumns = Lists.newArrayList(columns);
            allMeasures = Lists.newArrayList(measures);

            initiated = true;
        }
    }

    @Override
    public boolean isCapable(SQLDigest digest) {
        for (IRealization realization : getRealizations()) {
            if (realization.isCapable(digest))
                return true;
        }
        return false;
    }

    @Override
    public int getCost(SQLDigest digest) {
        return cost;
    }

    @Override
    public RealizationType getType() {
        return RealizationType.HYBRID;
    }

    @Override
    public String getFactTable() {
        return getRealizations()[0].getFactTable();
    }

    @Override
    public List<TblColRef> getAllColumns() {
        init();
        return allColumns;
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
    public String getProjectName() {
        return projectName;
    }

    @Override
    public void setProjectName(String prjName) {
        projectName = prjName;
    }

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
    public List<TblColRef> getAllDimensions() {
        init();
        return allDimensions;
    }

    public IRealization[] getRealizations() {
        init();
        return realizations;
    }

    public static String concatResourcePath(String hybridName) {
        return ResourceStore.HYBRID_RESOURCE_ROOT + "/" + hybridName + ".json";
    }

    public void setCost(int cost) {
        this.cost = cost;
    }
}
