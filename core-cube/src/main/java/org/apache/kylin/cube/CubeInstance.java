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

package org.apache.kylin.cube;

import static org.apache.kylin.shaded.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.kylin.cube.cuboid.CuboidModeEnum.CURRENT;
import static org.apache.kylin.cube.cuboid.CuboidModeEnum.RECOMMEND;
import static org.apache.kylin.cube.cuboid.CuboidModeEnum.CURRENT_WITH_BASE;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.CompressionUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.cuboid.CuboidModeEnum;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.cuboid.TreeCuboidScheduler;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IBuildable;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.CapabilityResult.CapabilityInfluence;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.kylin.shaded.com.google.common.base.Objects;
import org.apache.kylin.shaded.com.google.common.collect.Lists;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class CubeInstance extends RootPersistentEntity implements IRealization, IBuildable {
    private static final Logger logger = LoggerFactory.getLogger(CubeInstance.class);

    public static final int COST_WEIGHT_MEASURE = 1;
    public static final int COST_WEIGHT_DIMENSION = 10;
    public static final int COST_WEIGHT_INNER_JOIN = 100;

    public static CubeInstance create(String cubeName, CubeDesc cubeDesc) {
        CubeInstance cubeInstance = new CubeInstance();

        cubeInstance.setConfig((KylinConfigExt) cubeDesc.getConfig());
        cubeInstance.setName(cubeName);
        cubeInstance.setDisplayName(cubeName);
        cubeInstance.setDescName(cubeDesc.getName());
        cubeInstance.setCreateTimeUTC(System.currentTimeMillis());
        cubeInstance.setSegments(new Segments<CubeSegment>());
        cubeInstance.setStatus(RealizationStatusEnum.DISABLED);
        cubeInstance.updateRandomUuid();

        return cubeInstance;
    }

    @JsonIgnore
    private KylinConfigExt config;
    @JsonProperty("name")
    private String name;
    @JsonProperty("owner")
    private String owner;
    @JsonProperty("descriptor")
    private String descName;
    @JsonProperty("display_name")
    private String displayName;
    // DEPRECATED: the cost should be calculated in runtime
    // Mark cube priority for query
    @JsonProperty("cost")
    private int cost = 50;
    @JsonProperty("status")
    private RealizationStatusEnum status;

    @JsonManagedReference
    @JsonProperty("segments")
    private Segments<CubeSegment> segments = new Segments<CubeSegment>();

    @JsonProperty("create_time_utc")
    private long createTimeUTC;

    @JsonProperty("cuboid_bytes")
    private byte[] cuboidBytes;

    @JsonProperty("cuboid_bytes_recommend")
    private byte[] cuboidBytesRecommend;

    @JsonProperty("cuboid_last_optimized")
    private long cuboidLastOptimized;

    @JsonProperty("snapshots")
    private Map<String, String> snapshots = Maps.newHashMap();

    // cuboid scheduler lazy built
    transient private CuboidScheduler cuboidScheduler;

    // default constructor for jackson
    public CubeInstance() {
    }

    public CubeInstance latestCopyForWrite() {
        CubeManager mgr = CubeManager.getInstance(config);
        CubeInstance latest = mgr.getCube(name); // in case this object is out-of-date
        return mgr.copyForWrite(latest);
    }

    void init(KylinConfig config) {
        CubeDesc cubeDesc = CubeDescManager.getInstance(config).getCubeDesc(descName);
        checkNotNull(cubeDesc, "cube descriptor '%s' (for cube '%s') not found", descName, name);

        if (cubeDesc.isBroken()) {
            setStatus(RealizationStatusEnum.DESCBROKEN);
            logger.error("cube descriptor {} (for cube '{}') is broken", cubeDesc.getResourcePath(), name);
            logger.error("Errors: {}", cubeDesc.getErrorsAsString());
        } else if (getStatus() == RealizationStatusEnum.DESCBROKEN) {
            setStatus(RealizationStatusEnum.DISABLED);
            logger.info("cube {} changed from DESCBROKEN to DISABLED", name);
        }

        setConfig((KylinConfigExt) cubeDesc.getConfig());
    }

    public CuboidScheduler getCuboidScheduler() {
        if (cuboidScheduler != null)
            return cuboidScheduler;

        synchronized (this) {
            if (cuboidScheduler == null) {
                Map<Long, Long> cuboidsWithRowCnt = getCuboids();
                if (cuboidsWithRowCnt == null) {
                    cuboidScheduler = getDescriptor().getInitialCuboidScheduler();
                } else {
                    cuboidScheduler = new TreeCuboidScheduler(getDescriptor(),
                            Lists.newArrayList(cuboidsWithRowCnt.keySet()),
                            new TreeCuboidScheduler.CuboidCostComparator(cuboidsWithRowCnt));
                }
            }
        }
        return cuboidScheduler;
    }

    public Segments<CubeSegment> getBuildingSegments() {
        return segments.getBuildingSegments();
    }

    public Segments<CubeSegment> getMergingSegments(CubeSegment mergedSegment) {
        return segments.getMergingSegments(mergedSegment);
    }

    public CubeSegment getOriginalSegmentToRefresh(CubeSegment refreshedSegment) {
        return getOriginalSegment(refreshedSegment);
    }

    public CubeSegment getOriginalSegmentToOptimize(CubeSegment optimizedSegment) {
        return getOriginalSegment(optimizedSegment);
    }

    private CubeSegment getOriginalSegment(CubeSegment toSegment) {
        for (CubeSegment segment : this.getSegments(SegmentStatusEnum.READY)) {
            if (!toSegment.equals(segment) //
                    && toSegment.getSegRange().equals(segment.getSegRange())) {
                return segment;
            }
        }
        return null;
    }

    public CubeDesc getDescriptor() {
        if (config == null) {
            return null;
        }
        return CubeDescManager.getInstance(config).getCubeDesc(descName);
    }

    @Override
    public DataModelDesc getModel() {
        CubeDesc cubeDesc = this.getDescriptor();
        if (cubeDesc != null) {
            return cubeDesc.getModel();
        } else {
            return null;
        }
    }

    public boolean isReady() {
        return getStatus() == RealizationStatusEnum.READY;
    }

    // if cube is not online and has no data or any building job, we allow its descriptor to be
    // in a temporary broken state, so that user can edit and fix it. Broken state is often due to
    // schema changes at source.
    public boolean allowBrokenDescriptor() {
        return (getStatus() == RealizationStatusEnum.DISABLED || getStatus() == RealizationStatusEnum.DESCBROKEN)
                && segments.isEmpty();
    }

    @Override
    public String resourceName() {
        return name;
    }

    public String getResourcePath() {
        return concatResourcePath(resourceName());
    }

    public static String concatResourcePath(String cubeName) {
        return ResourceStore.CUBE_RESOURCE_ROOT + "/" + cubeName + ".json";
    }

    @Override
    public String toString() {
        return getCanonicalName();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null || getClass() != obj.getClass())
            return false;
        CubeInstance other = (CubeInstance) obj;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        return true;
    }

    public boolean equalsRaw(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        CubeInstance that = (CubeInstance) o;
        if (!java.util.Objects.equals(name, that.name))
            return false;
        if (!java.util.Objects.equals(owner, that.owner))
            return false;
        if (!java.util.Objects.equals(descName, that.descName))
            return false;
        if (!java.util.Objects.equals(displayName, that.displayName))
            return false;
        if (!java.util.Objects.equals(status, that.status))
            return false;

        if (!java.util.Objects.equals(segments, that.segments))
            return false;
        if (!java.util.Arrays.equals(cuboidBytes, that.cuboidBytes))
            return false;
        if (!java.util.Arrays.equals(cuboidBytesRecommend, that.cuboidBytesRecommend))
            return false;
        return java.util.Objects.equals(snapshots, that.snapshots);
    }

    // ============================================================================

    @Override
    public KylinConfig getConfig() {
        return config;
    }

    void setConfig(KylinConfigExt config) {
        this.config = config;
    }

    public String getRootFactTable() {
        return getModel().getRootFactTable().getTableIdentity();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getCanonicalName() {
        return getType() + "[name=" + name + "]";
    }

    @Override
    public List<MeasureDesc> getMeasures() {
        return getDescriptor().getMeasures();
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getDescName() {
        return descName;
    }

    public void setDescName(String descName) {
        this.descName = descName;
    }

    public String getDisplayName() {
        if (StringUtils.isBlank(displayName)) {
            displayName = name;
        }
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    @Override
    public int getCost() {
        if (getDescriptor() == null) {
            //in case not initialized
            return 0;
        }
        int countedDimensionNum = getRowKeyColumnCount();
        int c = countedDimensionNum * COST_WEIGHT_DIMENSION + getMeasures().size() * COST_WEIGHT_MEASURE;
        DataModelDesc model = getModel();
        if (model == null) {
            //in case broken cube
            return 0;
        }
        for (JoinTableDesc join : model.getJoinTables()) {
            if (join.getJoin().isInnerJoin())
                c += CubeInstance.COST_WEIGHT_INNER_JOIN;
        }
        return c;
    }

    public void setCost(int cost) {
        this.cost = cost;
    }

    public RealizationStatusEnum getStatus() {
        return status;
    }

    public void setStatus(RealizationStatusEnum status) {
        this.status = status;
    }

    public CubeSegment getFirstSegment() {
        return segments.getFirstSegment();
    }

    public CubeSegment getLatestReadySegment() {
        return segments.getLatestReadySegment();
    }

    public CubeSegment getLatestBuiltSegment() {
        return segments.getLatestBuiltSegment();
    }

    public Segments<CubeSegment> getSegments() {
        return segments;
    }

    public Segments<CubeSegment> getSegments(SegmentStatusEnum status) {
        return segments.getSegments(status);
    }

    public CubeSegment getSegment(String name, SegmentStatusEnum status) {
        return segments.getSegment(name, status);
    }

    public void setSegments(Segments segments) {
        this.segments = new Segments<>(segments);
    }

    public CubeSegment getSegmentById(String segmentId) {
        for (CubeSegment segment : segments) {
            if (Objects.equal(segment.getUuid(), segmentId)) {
                return segment;
            }
        }
        return null;
    }

    public CubeSegment[] regetSegments(CubeSegment... segs) {
        CubeSegment[] r = new CubeSegment[segs.length];
        for (int i = 0; i < segs.length; i++) {
            r[i] = getSegmentById(segs[i].getUuid());
        }
        return r;
    }

    public long getCreateTimeUTC() {
        return createTimeUTC;
    }

    public void setCreateTimeUTC(long createTimeUTC) {
        this.createTimeUTC = createTimeUTC;
    }

    public void clearCuboids() {
        cuboidBytes = null;
        cuboidBytesRecommend = null;
        cuboidLastOptimized = 0L;
    }

    public Set<Long> getCuboidsByMode(String cuboidModeName) {
        return getCuboidsByMode(cuboidModeName == null ? null : CuboidModeEnum.getByModeName(cuboidModeName));
    }

    public Set<Long> getCuboidsByMode(CuboidModeEnum cuboidMode) {
        Set<Long> currentCuboid = getCuboidScheduler().getAllCuboidIds();
        if (cuboidMode == null || cuboidMode == CURRENT) {
            return currentCuboid;
        }
        if (cuboidMode == CURRENT_WITH_BASE) {
            currentCuboid.add(getCuboidScheduler().getBaseCuboidId());
            return currentCuboid;
        }
        Set<Long> cuboidsRecommend = getCuboidsRecommend();
        if (cuboidsRecommend == null || cuboidMode == RECOMMEND) {
            return cuboidsRecommend;
        }
        Set<Long> currentCuboids = getCuboidScheduler().getAllCuboidIds();
        switch (cuboidMode) {
        case RECOMMEND_EXISTING:
            cuboidsRecommend.retainAll(currentCuboids);
            return cuboidsRecommend;
        case RECOMMEND_MISSING:
            cuboidsRecommend.removeAll(currentCuboids);
            return cuboidsRecommend;
        case RECOMMEND_MISSING_WITH_BASE:
            cuboidsRecommend.removeAll(currentCuboids);
            cuboidsRecommend.add(getCuboidScheduler().getBaseCuboidId());
            return cuboidsRecommend;
        default:
            return null;
        }
    }

    public Map<Long, Long> getCuboids() {
        if (cuboidBytes == null)
            return null;
        byte[] uncompressed;
        try {
            uncompressed = CompressionUtils.decompress(cuboidBytes);
            String str = new String(uncompressed, StandardCharsets.UTF_8);
            TypeReference<Map<Long, Long>> typeRef = new TypeReference<Map<Long, Long>>() {
            };
            Map<Long, Long> cuboids = JsonUtil.readValue(str, typeRef);
            return cuboids.isEmpty() ? null : cuboids;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void setCuboids(Map<Long, Long> cuboids) {
        if (cuboids == null)
            return;
        if (cuboids.isEmpty()) {
            cuboidBytes = null;
            return;
        }

        try {
            String str = JsonUtil.writeValueAsString(cuboids);
            byte[] compressed = CompressionUtils.compress(str.getBytes(StandardCharsets.UTF_8));
            cuboidBytes = compressed;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Set<Long> getCuboidsRecommend() {
        if (cuboidBytesRecommend == null)
            return null;
        byte[] uncompressed;
        try {
            uncompressed = CompressionUtils.decompress(cuboidBytesRecommend);
            String str = new String(uncompressed, StandardCharsets.UTF_8);
            TypeReference<Set<Long>> typeRef = new TypeReference<Set<Long>>() {
            };
            Set<Long> cuboids = JsonUtil.readValue(str, typeRef);
            return cuboids.isEmpty() ? null : cuboids;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void setCuboidsRecommend(Set<Long> cuboids) {
        if (cuboids == null)
            return;
        if (cuboids.isEmpty()) {
            cuboidBytesRecommend = null;
            return;
        }
        try {
            String str = JsonUtil.writeValueAsString(cuboids);
            byte[] compressed = CompressionUtils.compress(str.getBytes(StandardCharsets.UTF_8));
            cuboidBytesRecommend = compressed;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public long getCuboidLastOptimized() {
        return cuboidLastOptimized;
    }

    public void setCuboidLastOptimized(long lastOptimized) {
        this.cuboidLastOptimized = lastOptimized;
    }

    /**
     * Get cuboid level count except base cuboid
     *
     * @return
     */
    public int getBuildLevel() {
        return getCuboidScheduler().getCuboidsByLayer().size() - 1;
    }

    @Override
    public CapabilityResult isCapable(SQLDigest digest) {
        CapabilityResult result = CubeCapabilityChecker.check(this, digest);
        if (result.capable) {
            result.cost = getCost(digest);
            for (CapabilityInfluence i : result.influences) {
                double suggestCost = i.suggestCostMultiplier();
                result.cost *= (suggestCost == 0) ? 1.0 : suggestCost;
            }
        } else {
            result.cost = -1;
        }
        return result;
    }

    public int getCost(SQLDigest digest) {
        int calculatedCost = cost;

        //the number of dimensions is not as accurate as number of row key cols
        calculatedCost += getRowKeyColumnCount() * COST_WEIGHT_DIMENSION + getMeasures().size() * COST_WEIGHT_MEASURE;

        for (JoinTableDesc joinTable : this.getModel().getJoinTables()) {
            // more tables, more cost
            if (joinTable.getJoin().isInnerJoin()) {
                // inner join cost is bigger than left join, as it will filter some records
                calculatedCost += COST_WEIGHT_INNER_JOIN;
            }
        }

        return calculatedCost;
    }

    @Override
    public RealizationType getType() {
        return RealizationType.CUBE;
    }

    @Override
    public Set<TblColRef> getAllColumns() {
        return getDescriptor().listAllColumns();
    }

    @Override
    public Set<ColumnDesc> getAllColumnDescs() {
        return getDescriptor().listAllColumnDescs();
    }

    @Override
    public long getDateRangeStart() {
        return segments.getTSStart();
    }

    @Override
    public long getDateRangeEnd() {
        return segments.getTSEnd();
    }

    @Override
    public boolean supportsLimitPushDown() {
        return getDescriptor().supportsLimitPushDown();
    }

    public int getRowKeyColumnCount() {
        return getDescriptor().getRowkey().getRowKeyColumns().length;
    }

    @Override
    public List<TblColRef> getAllDimensions() {
        return Lists.newArrayList(getDescriptor().listDimensionColumnsIncludingDerived());
    }

    public boolean needAutoMerge() {
        if (!this.getDescriptor().getModel().getPartitionDesc().isPartitioned())
            return false;

        return this.getConfig().isAutoMergeEnabled() && this.getDescriptor().getAutoMergeTimeRanges() != null
                && this.getDescriptor().getAutoMergeTimeRanges().length > 0 && this.getStatus() == RealizationStatusEnum.READY;
    }

    public SegmentRange autoMergeCubeSegments() throws IOException {
        return segments.autoMergeCubeSegments(needAutoMerge(), getName(), getDescriptor().getAutoMergeTimeRanges(),
                getDescriptor().getVolatileRange());
    }

    public Segments calculateToBeSegments(CubeSegment newSegment) {
        return segments.calculateToBeSegments(newSegment);
    }

    public CubeSegment getLastSegment() {
        List<CubeSegment> existing = getSegments();
        if (existing.isEmpty()) {
            return null;
        } else {
            return existing.get(existing.size() - 1);
        }
    }

    // For JSON serialization of this attribute, use CubeInstanceResponse
    public long getSizeKB() {
        long sizeKb = 0L;

        for (CubeSegment cubeSegment : this.getSegments(SegmentStatusEnum.READY)) {
            sizeKb += cubeSegment.getSizeKB();
        }

        return sizeKb;
    }

    // For JSON serialization of this attribute, use CubeInstanceResponse
    public long getInputRecordCount() {
        long sizeRecordCount = 0L;

        for (CubeSegment cubeSegment : this.getSegments(SegmentStatusEnum.READY)) {
            sizeRecordCount += cubeSegment.getInputRecords();
        }

        return sizeRecordCount;
    }

    // For JSON serialization of this attribute, use CubeInstanceResponse
    public long getInputRecordSizeBytes() {
        long sizeRecordSize = 0L;

        for (CubeSegment cubeSegment : this.getSegments(SegmentStatusEnum.READY)) {
            sizeRecordSize += cubeSegment.getInputRecordsSize();
        }

        return sizeRecordSize;
    }

    public String getProject() {
        return getDescriptor().getProject();
    }

    public ProjectInstance getProjectInstance() {
        return ProjectManager.getInstance(getConfig()).getProject(getProject());
    }

    @Override
    public int getSourceType() {
        return getModel().getRootFactTable().getTableDesc().getSourceType();
    }

    @Override
    public int getStorageType() {
        return getDescriptor().getStorageType();
    }

    @Override
    public int getEngineType() {
        return getDescriptor().getEngineType();
    }

    public Map<String, String> getSnapshots() {
        if (snapshots == null)
            snapshots = Maps.newHashMap();
        return snapshots;
    }

    public void resetSnapshots() {
        snapshots = Maps.newHashMap();
    }

    public String getSnapshotResPath(String tableName) {
        return getSnapshots().get(tableName);
    }

    public void putSnapshotResPath(String table, String snapshotResPath) {
        getSnapshots().put(table, snapshotResPath);
    }

    public static CubeInstance getCopyOf(CubeInstance other) {
        CubeInstance ret = new CubeInstance();
        ret.setName(other.getName());
        ret.setOwner(other.getOwner());
        ret.setDescName(other.getDescName());
        ret.setCost(other.getCost());
        ret.setStatus(other.getStatus());
        ret.setSegments(other.getSegments());
        ret.setCreateTimeUTC(System.currentTimeMillis());
        if (other.cuboidBytes != null) {
            ret.cuboidBytes = Arrays.copyOf(other.cuboidBytes, other.cuboidBytes.length);
        }
        if (other.cuboidBytesRecommend != null) {
            ret.cuboidBytesRecommend = Arrays.copyOf(other.cuboidBytesRecommend, other.cuboidBytesRecommend.length);
        }
        ret.cuboidLastOptimized = other.cuboidLastOptimized;
        ret.getSnapshots().putAll(other.getSnapshots());

        ret.setConfig((KylinConfigExt) other.getConfig());
        ret.updateRandomUuid();
        return ret;
    }

    public static CubeSegment findSegmentWithJobId(String jobID, CubeInstance cubeInstance) {
        for (CubeSegment segment : cubeInstance.getSegments()) {
            String lastBuildJobID = segment.getLastBuildJobID();
            if (lastBuildJobID != null && lastBuildJobID.equalsIgnoreCase(jobID)) {
                return segment;
            }
        }
        throw new IllegalStateException("No segment's last build job ID equals " + jobID);
    }

}