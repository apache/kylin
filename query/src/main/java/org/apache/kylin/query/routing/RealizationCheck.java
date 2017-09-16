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

package org.apache.kylin.query.routing;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.query.relnode.OLAPTableScan;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class RealizationCheck {
    private Map<DataModelDesc, List<IncapableReason>> modelIncapableReasons = Maps.newHashMap();
    private Map<CubeDesc, IncapableReason> cubeIncapableReasons = Maps.newHashMap();
    private Map<CubeDesc, Boolean> cubeCapabilities = Maps.newHashMap();
    private List<DataModelDesc> capableModels = Lists.newArrayList();

    public Map<DataModelDesc, List<IncapableReason>> getModelIncapableReasons() {
        return modelIncapableReasons;
    }

    public Map<CubeDesc, IncapableReason> getCubeIncapableReasons() {
        return cubeIncapableReasons;
    }

    public Map<CubeDesc, Boolean> getCubeCapabilities() {
        return cubeCapabilities;
    }

    public void addCapableCube(IRealization realization) {
        if (realization instanceof CubeInstance) {
            cubeCapabilities.put(((CubeInstance) realization).getDescriptor(), true);
        }
    }

    public void addIncapableCube(IRealization realization) {
        if (realization instanceof CubeInstance) {
            cubeCapabilities.put(((CubeInstance) realization).getDescriptor(), false);
        }
    }

    public void addIncapableCube(IRealization realization, IncapableReason incapableReason) {
        if (realization instanceof CubeInstance) {
            cubeCapabilities.put(((CubeInstance) realization).getDescriptor(), false);
            cubeIncapableReasons.put(((CubeInstance) realization).getDescriptor(), incapableReason);
        }
    }

    public void addCubeIncapableReason(IRealization realization, IncapableReason incapableReason) {
        if (realization instanceof CubeInstance) {
            cubeIncapableReasons.put(((CubeInstance) realization).getDescriptor(), incapableReason);
        }
    }

    public List<DataModelDesc> getCapableModels() {
        return capableModels;
    }

    public void addModelIncapableReason(DataModelDesc modelDesc, IncapableReason reason) {
        if (!modelIncapableReasons.containsKey(modelDesc)) {
            List<IncapableReason> reasons = Lists.newArrayList(reason);
            modelIncapableReasons.put(modelDesc, reasons);
        } else {
            modelIncapableReasons.get(modelDesc).add(reason);
        }
    }

    public void addCapableModel(DataModelDesc modelDesc) {
        this.capableModels.add(modelDesc);
    }

    public void addModelIncapableReason(DataModelDesc modelDesc, List<IncapableReason> reasons) {
        modelIncapableReasons.put(modelDesc, reasons);
    }

    public static enum IncapableType {
        CUBE_NOT_READY, CUBE_NOT_CONTAIN_TABLE, CUBE_NOT_CONTAIN_ALL_COLUMN, CUBE_NOT_CONTAIN_ALL_DIMENSION, CUBE_NOT_CONTAIN_ALL_MEASURE, CUBE_BLACK_OUT_REALIZATION, CUBE_UN_SUPPORT_MASSIN, CUBE_UN_SUPPORT_RAWQUERY, CUBE_UNMATCHED_DIMENSION, CUBE_LIMIT_PRECEDE_AGGR, CUBE_UNMATCHED_AGGREGATION, CUBE_OTHER_CUBE_INCAPABLE, MODEL_UNMATCHED_JOIN, MODEL_JOIN_TYPE_UNMATCHED, MODEL_JOIN_CONDITION_UNMATCHED, MODEL_JOIN_NOT_FOUND, MODEL_BAD_JOIN_SEQUENCE, MODEL_FACT_TABLE_NOT_FOUND, MODEL_OTHER_MODEL_INCAPABLE
    }

    public static class IncapableReason {
        private IncapableType incapableType;
        // notFoundColumns = notFoundDimensions + notFoundMeasures;
        private Collection<TblColRef> notFoundColumns;
        private Collection<TblColRef> notFoundDimensions;
        private Collection<FunctionDesc> notFoundMeasures;
        private Collection<TblColRef> unmatchedDimensions;
        private Collection<FunctionDesc> unmatchedAggregations;
        private Collection<OLAPTableScan> notFoundTables;

        public static IncapableReason create(IncapableType incapableType) {
            IncapableReason incapableReason = new IncapableReason();
            incapableReason.setIncapableType(incapableType);
            return incapableReason;
        }

        public static IncapableReason create(CapabilityResult.IncapableCause incapableCause) {
            if (incapableCause == null) {
                return null;
            }
            IncapableReason incapableReason = new IncapableReason();
            IncapableType incapableType = null;
            switch (incapableCause.getIncapableType()) {
            case UNSUPPORT_MASSIN:
                incapableType = IncapableType.CUBE_UN_SUPPORT_MASSIN;
                break;
            case UNMATCHED_DIMENSION:
                incapableType = IncapableType.CUBE_UNMATCHED_DIMENSION;
                break;
            case LIMIT_PRECEDE_AGGR:
                incapableType = IncapableType.CUBE_LIMIT_PRECEDE_AGGR;
                break;
            case UNMATCHED_AGGREGATION:
                incapableType = IncapableType.CUBE_UNMATCHED_AGGREGATION;
                break;
            case UNSUPPORT_RAWQUERY:
                incapableType = IncapableType.CUBE_UN_SUPPORT_RAWQUERY;
                break;
            case II_UNMATCHED_FACT_TABLE:
                incapableType = IncapableType.MODEL_FACT_TABLE_NOT_FOUND;
                break;
            case II_MISSING_COLS:
                incapableType = IncapableType.CUBE_NOT_CONTAIN_ALL_COLUMN;
                break;
            default:
                break;
            }
            incapableReason.setIncapableType(incapableType);
            incapableReason.setUnmatchedDimensions(incapableCause.getUnmatchedDimensions());
            incapableReason.setUnmatchedAggregations(incapableCause.getUnmatchedAggregations());
            return incapableReason;
        }

        public static IncapableReason notContainAllColumn(Collection<TblColRef> notFoundColumns) {
            IncapableReason incapableReason = new IncapableReason();
            incapableReason.setIncapableType(IncapableType.CUBE_NOT_CONTAIN_ALL_COLUMN);
            incapableReason.setNotFoundColumns(notFoundColumns);
            return incapableReason;
        }

        public static IncapableReason notContainAllDimension(Collection<TblColRef> notFoundDimensions) {
            IncapableReason incapableReason = new IncapableReason();
            incapableReason.setIncapableType(IncapableType.CUBE_NOT_CONTAIN_ALL_DIMENSION);
            incapableReason.setNotFoundDimensions(notFoundDimensions);
            return incapableReason;
        }

        public static IncapableReason notContainAllMeasures(Collection<FunctionDesc> notFoundMeasures) {
            IncapableReason incapableReason = new IncapableReason();
            incapableReason.setIncapableType(IncapableType.CUBE_NOT_CONTAIN_ALL_MEASURE);
            incapableReason.setNotFoundMeasures(notFoundMeasures);
            return incapableReason;
        }

        public static IncapableReason notFoundTables(Collection<OLAPTableScan> notFoundTables) {
            IncapableReason incapableReason = new IncapableReason();
            incapableReason.setIncapableType(IncapableType.CUBE_NOT_CONTAIN_TABLE);
            incapableReason.setNotFoundTables(notFoundTables);
            return incapableReason;
        }

        public void setIncapableType(IncapableType incapableType) {
            this.incapableType = incapableType;
        }

        public void setUnmatchedDimensions(Collection<TblColRef> unmatchedDimensions) {
            this.unmatchedDimensions = unmatchedDimensions;
        }

        public void setUnmatchedAggregations(Collection<FunctionDesc> unmatchedAggregations) {
            this.unmatchedAggregations = unmatchedAggregations;
        }

        public void setNotFoundColumns(Collection<TblColRef> notFoundColumns) {
            this.notFoundColumns = notFoundColumns;
        }

        public void setNotFoundTables(Collection<OLAPTableScan> notFoundTables) {
            this.notFoundTables = notFoundTables;
        }

        public void setNotFoundDimensions(Collection<TblColRef> notFoundDimensions) {
            this.notFoundDimensions = notFoundDimensions;
        }

        public void setNotFoundMeasures(Collection<FunctionDesc> notFoundMeasures) {
            this.notFoundMeasures = notFoundMeasures;
        }

        public Collection<TblColRef> getNotFoundDimensions() {
            return notFoundDimensions;
        }

        public Collection<FunctionDesc> getNotFoundMeasures() {
            return notFoundMeasures;
        }

        public IncapableType getIncapableType() {
            return incapableType;
        }

        public Collection<TblColRef> getUnmatchedDimensions() {
            return unmatchedDimensions;
        }

        public Collection<TblColRef> getNotFoundColumns() {
            return notFoundColumns;
        }

        public Collection<FunctionDesc> getUnmatchedAggregations() {
            return unmatchedAggregations;
        }

        public Collection<OLAPTableScan> getNotFoundTables() {
            return notFoundTables;
        }
    }
}
