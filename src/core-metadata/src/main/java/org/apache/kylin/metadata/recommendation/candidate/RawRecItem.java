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

package org.apache.kylin.metadata.recommendation.candidate;

import static org.apache.commons.lang3.time.DateUtils.MILLIS_PER_DAY;

import java.io.IOException;
import java.util.Locale;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.annotation.Clarification;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.recommendation.entity.CCRecItemV2;
import org.apache.kylin.metadata.recommendation.entity.DimensionRecItemV2;
import org.apache.kylin.metadata.recommendation.entity.LayoutRecItemV2;
import org.apache.kylin.metadata.recommendation.entity.MeasureRecItemV2;
import org.apache.kylin.metadata.recommendation.entity.RecItemV2;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.val;

@Getter
@Setter
@ToString
@Clarification(priority = Clarification.Priority.MAJOR, msg = "Enterprise")
public class RawRecItem {
    public static final String IMPORTED = "IMPORTED";
    public static final String QUERY_HISTORY = "QUERY_HISTORY";

    private static final String TYPE_ERROR_FORMAT = "incorrect raw recommendation type(%d), type value must from 1 to 4 included";
    private static final String STATE_ERROR_FORMAT = "incorrect raw recommendation state(%d), type value must from 0 to 4 included";

    private int id;
    private String project;
    private String modelID;
    private String uniqueFlag;
    private int semanticVersion;
    private RawRecType type;
    private RecItemV2 recEntity;
    private RawRecState state;
    private long createTime;
    private long updateTime;
    private int[] dependIDs;

    // only for raw layout recommendation
    private LayoutMetric layoutMetric;
    private int hitCount;
    private double cost;
    private double totalLatencyOfLastDay;
    private double totalTime;
    private double maxTime;
    private double minTime;
    private String queryHistoryInfo;
    private String recSource;

    // reserved fields
    private String reservedField2;
    private String reservedField3;

    public RawRecItem() {
    }

    public RawRecItem(String project, String modelID, int semanticVersion, RawRecType type) {
        this();
        this.project = project;
        this.modelID = modelID;
        this.semanticVersion = semanticVersion;
        this.type = type;
    }

    @JsonIgnore
    public boolean isOutOfDate(int semanticVersion) {
        return getSemanticVersion() < semanticVersion;
    }

    @JsonIgnore
    public boolean isAgg() {
        Preconditions.checkState(this.isLayoutRec());
        return ((LayoutRecItemV2) getRecEntity()).isAgg();
    }

    @JsonIgnore
    public boolean isLayoutRec() {
        return RawRecType.ADDITIONAL_LAYOUT == getType() || RawRecType.REMOVAL_LAYOUT == getType();
    }

    @JsonIgnore
    public boolean isAddLayoutRec() {
        return getType() == RawRecType.ADDITIONAL_LAYOUT;
    }

    @JsonIgnore
    public boolean isRemoveLayoutRec() {
        return getType() == RawRecType.REMOVAL_LAYOUT;
    }

    @JsonIgnore
    public boolean isAdditionalRecItemSavable() {
        Preconditions.checkState(isAddLayoutRec());
        if (RawRecItem.IMPORTED.equalsIgnoreCase(recSource)) {
            return true;
        }
        return getLayoutMetric() != null;
    }

    public void cleanLayoutStatistics() {
        this.setLayoutMetric(null);
        this.setHitCount(0);
        this.setCost(0);
        this.setTotalLatencyOfLastDay(0);
        this.setTotalTime(0);
        this.setMaxTime(0);
        this.setMinTime(0);
        this.setQueryHistoryInfo(null);
    }

    public void restoreIfNeed() {
        if (state == RawRecState.DISCARD) {
            state = RawRecState.INITIAL;
        }
    }

    public void updateCost(CostMethod costMethod, long currentTime, int effectiveDays) {
        long dayStart = getDateInMillis(currentTime);
        double newCost = 0;
        if (costMethod == CostMethod.HIT_COUNT) {
            val frequencyMap = getLayoutMetric().getFrequencyMap().getDateFrequency();
            for (int days = 0; days < effectiveDays; days++) {
                newCost += frequencyMap.getOrDefault(dayStart - days * MILLIS_PER_DAY, 0);
            }
        } else {
            LayoutMetric.LatencyMap latencyMap = getLayoutMetric().getLatencyMap();
            for (int days = 0; days < effectiveDays; days++) {
                newCost += latencyMap.getLatencyByDate(dayStart - days * MILLIS_PER_DAY) / (Math.pow(Math.E, days));
            }
        }
        setCost(newCost);
    }

    private long getDateInMillis(final long queryTime) {
        return TimeUtil.getDayStart(queryTime);
    }

    /**
     * Raw recommendation type
     */
    public enum RawRecType {
        COMPUTED_COLUMN(1), DIMENSION(2), MEASURE(3), ADDITIONAL_LAYOUT(4), REMOVAL_LAYOUT(5);

        private final int id;

        public int id() {
            return this.id;
        }

        RawRecType(int id) {
            this.id = id;
        }
    }

    /**
     * Raw recommendation state
     */
    public enum RawRecState {
        INITIAL(0), RECOMMENDED(1), APPLIED(2), DISCARD(3), BROKEN(4);

        private final int id;

        public int id() {
            return this.id;
        }

        RawRecState(int id) {
            this.id = id;
        }
    }

    public static int[] toDependIds(String jsonString) {
        try {
            return JsonUtil.readValue(jsonString, int[].class);
        } catch (IOException e) {
            throw new IllegalStateException("cannot deserialize depend id correctly", e);
        }
    }

    public static RawRecItem.RawRecType toRecType(byte recType) {
        switch (recType) {
        case 1:
            return RawRecItem.RawRecType.COMPUTED_COLUMN;
        case 2:
            return RawRecItem.RawRecType.DIMENSION;
        case 3:
            return RawRecItem.RawRecType.MEASURE;
        case 4:
            return RawRecItem.RawRecType.ADDITIONAL_LAYOUT;
        case 5:
            return RawRecItem.RawRecType.REMOVAL_LAYOUT;
        default:
            throw new IllegalStateException(String.format(Locale.ROOT, RawRecItem.TYPE_ERROR_FORMAT, recType));
        }
    }

    public static RawRecItem.RawRecState toRecState(byte stateType) {
        switch (stateType) {
        case 0:
            return RawRecItem.RawRecState.INITIAL;
        case 1:
            return RawRecItem.RawRecState.RECOMMENDED;
        case 2:
            return RawRecItem.RawRecState.APPLIED;
        case 3:
            return RawRecItem.RawRecState.DISCARD;
        case 4:
            return RawRecItem.RawRecState.BROKEN;
        default:
            throw new IllegalStateException(String.format(Locale.ROOT, RawRecItem.STATE_ERROR_FORMAT, stateType));
        }
    }

    public static RecItemV2 toRecItem(String jsonString, byte recType) {
        try {
            switch (recType) {
            case 1:
                return JsonUtil.readValue(jsonString, CCRecItemV2.class);
            case 2:
                return JsonUtil.readValue(jsonString, DimensionRecItemV2.class);
            case 3:
                return JsonUtil.readValue(jsonString, MeasureRecItemV2.class);
            case 4:
            case 5:
                return JsonUtil.readValue(jsonString, LayoutRecItemV2.class);
            default:
                throw new IllegalStateException(String.format(Locale.ROOT, RawRecItem.TYPE_ERROR_FORMAT, recType));
            }
        } catch (IOException | IllegalStateException e) {
            throw new IllegalStateException("cannot deserialize recommendation entity.", e);
        }
    }

    @JsonIgnore
    public IndexRecType getLayoutRecType() {
        Preconditions.checkArgument(this.isLayoutRec());
        if (isAgg() && isAddLayoutRec()) {
            return IndexRecType.ADD_AGG_INDEX;
        } else if (isAgg() && isRemoveLayoutRec()) {
            return IndexRecType.REMOVE_AGG_INDEX;
        } else if (isAddLayoutRec()) {
            return IndexRecType.ADD_TABLE_INDEX;
        } else {
            return IndexRecType.REMOVE_TABLE_INDEX;
        }
    }

    public enum IndexRecType {
        ADD_AGG_INDEX, REMOVE_AGG_INDEX, ADD_TABLE_INDEX, REMOVE_TABLE_INDEX
    }

    public enum CostMethod {
        HIT_COUNT, TIME_DECAY;

        public static CostMethod getCostMethod(String project) {
            String method = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(project)
                    .getConfig().getRecommendationCostMethod();
            return CostMethod.valueOf(method);
        }
    }
}
