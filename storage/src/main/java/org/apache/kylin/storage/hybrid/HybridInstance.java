package org.apache.kylin.storage.hybrid;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationRegistry;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.metadata.realization.SQLDigest;

import java.util.List;

/**
 */

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class HybridInstance extends RootPersistentEntity implements IRealization {

    @JsonIgnore
    private KylinConfig config;

    @JsonProperty("name")
    private String name;

    @JsonProperty("historyRealization")
    private RealizationEntry historyRealization;

    @JsonProperty("realTimeRealization")
    private RealizationEntry realTimeRealization;

    private IRealization historyRealizationInstance;
    private IRealization realTimeRealizationInstance;
    private String projectName;

    public void init() {
        RealizationRegistry registry = RealizationRegistry.getInstance(config);
        historyRealizationInstance = registry.getRealization(historyRealization.getType(), historyRealization.getRealization());
        realTimeRealizationInstance = registry.getRealization(realTimeRealization.getType(), realTimeRealization.getRealization());

        if (historyRealizationInstance == null) {
            throw new IllegalArgumentException("Didn't find realization '" + historyRealization.getType() + "'" + " with name '" + historyRealization.getRealization() + "' in '" + name + "'");
        }

        if (realTimeRealizationInstance == null) {
            throw new IllegalArgumentException("Didn't find realization '" + realTimeRealization.getType() + "'" + " with name '" + realTimeRealization.getRealization() + "' in '" + name + "'");
        }

    }

    @Override
    public boolean isCapable(SQLDigest digest) {
        return getHistoryRealizationInstance().isCapable(digest) && getRealTimeRealizationInstance().isCapable(digest);
    }

    @Override
    public int getCost(SQLDigest digest) {
        return historyRealizationInstance.getCost(digest);
        //return Math.min(historyRealizationInstance.getCost(digest), realTimeRealizationInstance.getCost(digest));
    }

    @Override
    public RealizationType getType() {
        return RealizationType.HYBRID;
    }

    @Override
    public String getFactTable() {
        return getHistoryRealizationInstance().getFactTable();
    }

    @Override
    public List<TblColRef> getAllColumns() {
        return getHistoryRealizationInstance().getAllColumns();
    }

    @Override
    public List<MeasureDesc> getMeasures() {
        return getHistoryRealizationInstance().getMeasures();
    }

    @Override
    public boolean isReady() {
        return historyRealizationInstance.isReady() || realTimeRealizationInstance.isReady();
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

    public RealizationEntry getHistoryRealization() {
        return historyRealization;
    }

    public RealizationEntry getRealTimeRealization() {
        return realTimeRealization;
    }

    public IRealization getHistoryRealizationInstance() {
        if (historyRealizationInstance == null) {
            this.init();
        }
        return historyRealizationInstance;
    }

    public IRealization getRealTimeRealizationInstance() {
        if (realTimeRealizationInstance == null) {
            this.init();
        }
        return realTimeRealizationInstance;
    }

    @Override
    public long getDateRangeStart() {
        return Math.min(getHistoryRealizationInstance().getDateRangeStart(), getRealTimeRealizationInstance().getDateRangeStart());
    }

    @Override
    public long getDateRangeEnd() {
        return Math.max(getHistoryRealizationInstance().getDateRangeEnd(), getRealTimeRealizationInstance().getDateRangeEnd()) +1;
    }

    

    public DataModelDesc getDataModelDesc(){
        if (getHistoryRealizationInstance() instanceof CubeInstance) {
            return ((CubeInstance) historyRealizationInstance).getDescriptor().getModel();
        }

        return ((IIInstance) historyRealizationInstance).getDescriptor().getModel();
    }

   @Override
    public String getModelName() {
        if (getHistoryRealizationInstance() instanceof CubeInstance) {
            return ((CubeInstance) historyRealizationInstance).getDescriptor().getModelName();
        }

        return ((IIInstance) historyRealizationInstance).getDescriptor().getModelName();
    }

    @Override
    public List<TblColRef> getAllDimensions(){

        return this.getHistoryRealizationInstance().getAllDimensions();
    }

}
