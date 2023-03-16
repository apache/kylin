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

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.query.relnode.OLAPTableScan;
import org.apache.kylin.metadata.model.NDataModel;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;

public class RealizationCheck {
    private Map<NDataModel, List<IncapableReason>> modelIncapableReasons = Maps.newHashMap();
    private Map<NDataModel, Map<String, String>> capableModels = Maps.newHashMap();

    public Map<NDataModel, List<IncapableReason>> getModelIncapableReasons() {
        return modelIncapableReasons;
    }

    public void addCapableCube(IRealization realization) {
        //empty
    }

    public void addIncapableCube(IRealization realization) {
        //empty
    }

    public void addIncapableCube(IRealization realization, IncapableReason incapableReason) {
        //empty
    }

    public void addCubeIncapableReason(IRealization realization, IncapableReason incapableReason) {
        //empty
    }

    public Map<NDataModel, Map<String, String>> getCapableModels() {
        return capableModels;
    }

    public void addModelIncapableReason(NDataModel modelDesc, IncapableReason reason) {
        if (!modelIncapableReasons.containsKey(modelDesc)) {
            List<IncapableReason> reasons = Lists.newArrayList(reason);
            modelIncapableReasons.put(modelDesc, reasons);
        } else {
            List<IncapableReason> incapableReasons = modelIncapableReasons.get(modelDesc);
            if (!incapableReasons.contains(reason))
                incapableReasons.add(reason);
        }
    }

    public void addCapableModel(NDataModel modelDesc, Map<String, String> aliasMap) {
        if (!this.capableModels.containsKey(modelDesc))
            this.capableModels.put(modelDesc, aliasMap);
    }

    public void addModelIncapableReason(NDataModel modelDesc, List<IncapableReason> reasons) {
        modelIncapableReasons.put(modelDesc, reasons);
    }

    public boolean isModelCapable() {
        return (!capableModels.isEmpty()) || modelIncapableReasons.isEmpty();
    }

    public boolean isCubeCapable() {
        return true;
    }

    public boolean isCapable() {
        return isModelCapable() && isCubeCapable();
    }

    public enum IncapableType {

        CUBE_NOT_READY, //cube not ready
        CUBE_NOT_CONTAIN_TABLE, // cube not contain table
        CUBE_NOT_CONTAIN_ALL_COLUMN, //
        CUBE_NOT_CONTAIN_ALL_DIMENSION, //
        CUBE_NOT_CONTAIN_ALL_MEASURE, //
        CUBE_BLACK_OUT_REALIZATION, //
        CUBE_UN_SUPPORT_MASSIN, //
        CUBE_UN_SUPPORT_RAWQUERY, //
        CUBE_UNMATCHED_DIMENSION, //
        CUBE_LIMIT_PRECEDE_AGGR, //
        CUBE_UNMATCHED_AGGREGATION, //
        CUBE_OTHER_CUBE_INCAPABLE, //
        TABLE_INDEX_NOT_CONTAIN_ALL_COLUMN, //
        // model
        MODEL_UNMATCHED_JOIN, //
        MODEL_JOIN_TYPE_UNMATCHED, //
        MODEL_JOIN_CONDITION_UNMATCHED, //
        MODEL_JOIN_NOT_FOUND, //
        MODEL_BAD_JOIN_SEQUENCE, //
        MODEL_FACT_TABLE_NOT_FOUND, //
        MODEL_OTHER_MODEL_INCAPABLE, //
        FACT_TABLE_NOT_CONSISTENT_IN_MODEL_AND_QUERY, MODEL_NOT_CONTAIN_ALL_COLUMN
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
            case TABLE_INDEX_MISSING_COLS:
                incapableType = IncapableType.TABLE_INDEX_NOT_CONTAIN_ALL_COLUMN;
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
            incapableReason.setIncapableType(IncapableType.MODEL_NOT_CONTAIN_ALL_COLUMN);
            incapableReason.setNotFoundColumns(notFoundColumns);
            return incapableReason;
        }

        public static IncapableReason notFoundTables(Collection<OLAPTableScan> notFoundTables) {
            IncapableReason incapableReason = new IncapableReason();
            incapableReason.setIncapableType(IncapableType.CUBE_NOT_CONTAIN_TABLE);
            incapableReason.setNotFoundTables(notFoundTables);
            return incapableReason;
        }

        public Collection<TblColRef> getNotFoundDimensions() {
            return notFoundDimensions;
        }

        public void setNotFoundDimensions(Collection<TblColRef> notFoundDimensions) {
            this.notFoundDimensions = notFoundDimensions;
        }

        public Collection<FunctionDesc> getNotFoundMeasures() {
            return notFoundMeasures;
        }

        public void setNotFoundMeasures(Collection<FunctionDesc> notFoundMeasures) {
            this.notFoundMeasures = notFoundMeasures;
        }

        public IncapableType getIncapableType() {
            return incapableType;
        }

        public void setIncapableType(IncapableType incapableType) {
            this.incapableType = incapableType;
        }

        public Collection<TblColRef> getUnmatchedDimensions() {
            return unmatchedDimensions;
        }

        public void setUnmatchedDimensions(Collection<TblColRef> unmatchedDimensions) {
            this.unmatchedDimensions = unmatchedDimensions;
        }

        public Collection<TblColRef> getNotFoundColumns() {
            return notFoundColumns;
        }

        public void setNotFoundColumns(Collection<TblColRef> notFoundColumns) {
            this.notFoundColumns = notFoundColumns;
        }

        public Collection<FunctionDesc> getUnmatchedAggregations() {
            return unmatchedAggregations;
        }

        public void setUnmatchedAggregations(Collection<FunctionDesc> unmatchedAggregations) {
            this.unmatchedAggregations = unmatchedAggregations;
        }

        public Collection<OLAPTableScan> getNotFoundTables() {
            return notFoundTables;
        }

        public void setNotFoundTables(Collection<OLAPTableScan> notFoundTables) {
            this.notFoundTables = notFoundTables;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            IncapableReason that = (IncapableReason) o;

            if (incapableType != that.incapableType)
                return false;
            if (notFoundColumns != null ? !notFoundColumns.equals(that.notFoundColumns) : that.notFoundColumns != null)
                return false;
            if (notFoundDimensions != null ? !notFoundDimensions.equals(that.notFoundDimensions)
                    : that.notFoundDimensions != null)
                return false;
            if (notFoundMeasures != null ? !notFoundMeasures.equals(that.notFoundMeasures)
                    : that.notFoundMeasures != null)
                return false;
            if (unmatchedDimensions != null ? !unmatchedDimensions.equals(that.unmatchedDimensions)
                    : that.unmatchedDimensions != null)
                return false;
            if (unmatchedAggregations != null ? !unmatchedAggregations.equals(that.unmatchedAggregations)
                    : that.unmatchedAggregations != null)
                return false;
            return notFoundTables != null ? notFoundTables.equals(that.notFoundTables) : that.notFoundTables == null;
        }

        @Override
        public int hashCode() {
            int result = incapableType != null ? incapableType.hashCode() : 0;
            result = 31 * result + (notFoundColumns != null ? notFoundColumns.hashCode() : 0);
            result = 31 * result + (notFoundDimensions != null ? notFoundDimensions.hashCode() : 0);
            result = 31 * result + (notFoundMeasures != null ? notFoundMeasures.hashCode() : 0);
            result = 31 * result + (unmatchedDimensions != null ? unmatchedDimensions.hashCode() : 0);
            result = 31 * result + (unmatchedAggregations != null ? unmatchedAggregations.hashCode() : 0);
            result = 31 * result + (notFoundTables != null ? notFoundTables.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(incapableType.toString());

            switch (incapableType) {
            case CUBE_NOT_CONTAIN_TABLE:
                if (notFoundTables != null) {
                    sb.append('[');
                    sb.append(StringUtils.join(notFoundTables.toArray(), ", "));
                    sb.append(']');
                }
                break;
            case CUBE_NOT_CONTAIN_ALL_COLUMN:
                if (notFoundColumns != null) {
                    sb.append('[');
                    sb.append(StringUtils.join(notFoundColumns.toArray(), ", "));
                    sb.append(']');
                }
                break;
            case CUBE_NOT_CONTAIN_ALL_DIMENSION:
                if (notFoundDimensions != null) {
                    sb.append('[');
                    sb.append(StringUtils.join(notFoundDimensions.toArray(), ", "));
                    sb.append(']');
                }
                break;
            case CUBE_NOT_CONTAIN_ALL_MEASURE:
                if (notFoundMeasures != null) {
                    sb.append('[');
                    sb.append(StringUtils.join(notFoundMeasures.toArray(), ", "));
                    sb.append(']');
                }
                break;
            case CUBE_UNMATCHED_DIMENSION:
                if (unmatchedDimensions != null) {
                    sb.append('[');
                    sb.append(StringUtils.join(unmatchedDimensions.toArray(), ", "));
                    sb.append(']');
                }
                break;
            case CUBE_UNMATCHED_AGGREGATION:
                if (unmatchedAggregations != null) {
                    sb.append('[');
                    sb.append(StringUtils.join(unmatchedAggregations.toArray(), ", "));
                    sb.append(']');
                }
                break;
            case CUBE_NOT_READY:
            case CUBE_BLACK_OUT_REALIZATION:
            case CUBE_UN_SUPPORT_MASSIN:
            case CUBE_UN_SUPPORT_RAWQUERY:
            case CUBE_LIMIT_PRECEDE_AGGR:
            case CUBE_OTHER_CUBE_INCAPABLE:
                break;
            case MODEL_UNMATCHED_JOIN:
            case MODEL_JOIN_TYPE_UNMATCHED:
            case MODEL_JOIN_CONDITION_UNMATCHED:
            case MODEL_JOIN_NOT_FOUND:
            case MODEL_BAD_JOIN_SEQUENCE:
            case MODEL_FACT_TABLE_NOT_FOUND:
            case MODEL_OTHER_MODEL_INCAPABLE:
                break;
            default:
                break;
            }
            return sb.toString();
        }
    }
}
