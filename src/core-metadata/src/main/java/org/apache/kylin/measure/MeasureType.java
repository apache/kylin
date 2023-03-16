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

package org.apache.kylin.measure;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult.CapabilityInfluence;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.Tuple;
import org.apache.kylin.metadata.tuple.TupleInfo;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

/**
 * MeasureType captures how a kind of aggregation is defined, how it is calculated
 * during cube build, and how it is involved in query and storage scan.
 *
 * @param <T> the Java type of aggregation data object, e.g. HLLCounter
 */
abstract public class MeasureType<T> implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    /* ============================================================================
     * Define
     * ---------------------------------------------------------------------------- */

    /** Validates a user defined FunctionDesc has expected parameter etc. Throw IllegalArgumentException if anything wrong. */
    public void validate(FunctionDesc functionDesc) throws IllegalArgumentException {
        return;
    }

    /** Although most aggregated object takes only 8 bytes like long or double,
     * some advanced aggregation like HyperLogLog or TopN can consume more than 10 KB for
     * each object, which requires special care on memory allocation. */
    public boolean isMemoryHungry() {
        return false;
    }

    /** Return true if this MeasureType only aggregate values in base cuboid, and output initial value in child cuboid. */
    public boolean onlyAggrInBaseCuboid() {
        return false;
    }

    /* ============================================================================
     * Build
     * ---------------------------------------------------------------------------- */

    /** Return a MeasureIngester which knows how to init aggregation object from raw records. */
    abstract public MeasureIngester<T> newIngester();

    /** Return a MeasureAggregator which does aggregation. */
    abstract public MeasureAggregator<T> newAggregator();

    /** Some special measures need dictionary to encode column values for optimal storage. TopN is an example. */
    public List<TblColRef> getColumnsNeedDictionary(FunctionDesc functionDesc) {
        return Collections.emptyList();
    }

    /* ============================================================================
     * Cube Selection
     * ---------------------------------------------------------------------------- */

    /**
     * Some special measures hold columns which are usually treated as dimensions (or vice-versa).
     * This is where they override to influence cube capability check.
     *
     * A SQLDigest contains dimensions and measures extracted from a query. After comparing to
     * cube definition, the matched dimensions and measures are crossed out, and what's left is
     * the <code>unmatchedDimensions</code> and <code>unmatchedAggregations</code>.
     *
     * Each measure type on the cube is then called on this method to check if any of the unmatched
     * can be fulfilled. If a measure type cannot fulfill any of the unmatched, it simply return null.
     * Or otherwise, <code>unmatchedDimensions</code> and <code>unmatchedAggregations</code> must
     * be modified to drop the satisfied dimension or measure, and a CapabilityInfluence object
     * must be returned to mark the contribution of this measure type.
     */
    public CapabilityInfluence influenceCapabilityCheck(Collection<TblColRef> unmatchedDimensions,
            Collection<FunctionDesc> unmatchedAggregations, SQLDigest digest, MeasureDesc measureDesc) {
        return null;
    }

    /* ============================================================================
     * Query Rewrite
     * ---------------------------------------------------------------------------- */

    /** Whether or not Calcite rel-tree needs rewrite to do last around of aggregation */
    abstract public boolean needRewrite();

    /** Does the rewrite involves an extra field to hold the pre-calculated */
    public boolean needRewriteField() {
        return true;
    }

    /**
     * Returns a map from UDAF to Calcite aggregation function implementation class.
     * There can be zero or more UDAF defined on a measure type.
     */
    public Map<String, Class<?>> getRewriteCalciteAggrFunctions() {
        return null;
    }

    /* ============================================================================
     * Storage
     * ---------------------------------------------------------------------------- */

    /**
     * Some special measures hold columns which are usually treated as dimensions (or vice-versa).
     * They need to adjust dimensions and measures in <code>sqlDigest</code> before scanning,
     * such that correct cuboid and measures can be selected by storage.
     */
    public void adjustSqlDigest(MeasureDesc involvedMeasure, SQLDigest sqlDigest) {
    }

    /** Return true if one storage record maps to multiple tuples, or false otherwise. */
    public boolean needAdvancedTupleFilling() {
        return false;
    }

    /** The simple filling mode, one tuple per storage record. */
    public void fillTupleSimply(Tuple tuple, int indexInTuple, Object measureValue) {
        tuple.setMeasureValue(indexInTuple, measureValue);
    }

    /** The advanced filling mode, multiple tuples per storage record. */
    public IAdvMeasureFiller getAdvancedTupleFiller(FunctionDesc function, TupleInfo returnTupleInfo,
            Map<TblColRef, Dictionary<String>> dictionaryMap) {
        throw new UnsupportedOperationException();
    }

    public static interface IAdvMeasureFiller {

        /** Reload a value from storage and get ready to fill multiple tuples with it. */
        void reload(Object measureValue);

        /** Returns how many rows contained in last loaded value. */
        int getNumOfRows();

        /** Fill in specified row into tuple. */
        void fillTuple(Tuple tuple, int row);
    }

    public boolean expandable() {
        return false;
    }

    /**
     * A user defined measure (outer measure) could translate into one or more internal measures which are the
     * actual measures get computed and stored in cube.
     */
    public List<FunctionDesc> convertToInternalFunctionDesc(FunctionDesc functionDesc) {
        return Lists.newArrayList(functionDesc);
    }
}
