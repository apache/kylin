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

package org.apache.kylin.storage.hbase;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.kv.RowKeyColumnIO;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;

import java.util.Collection;
import java.util.LinkedList;

/**
 * @author Huang, Hua
 */
public class TupleFilterValueOptimizer {

    private static boolean isInDictionary(Dictionary<String> dict, String value) {
        boolean inFlag = true;
        try {
            int id = dict.getIdFromValue(value, 0);
        } catch (IllegalArgumentException ex) {
            inFlag = false;
        }

        return inFlag;
    }

    private static Collection<String> removeNonDictionaryValues(CubeSegment cubeSegment, TblColRef column, Collection<String> values) {
        RowKeyColumnIO rowKeyColumnIO = new RowKeyColumnIO(cubeSegment);

        Dictionary<String> dict = rowKeyColumnIO.getDictionary(column);
        // in case that dict is null, just return values
        if (dict == null) return values;

        Collection<String> newValues = new LinkedList<String>();
        for (String value : values) {
            if (isInDictionary(dict, value)) newValues.add(value);
        }

        return newValues;
    }

    private static Collection<String> roundDictionaryValues(CubeSegment cubeSegment, TblColRef column, Collection<String> values, int roundingFlag) {
        RowKeyColumnIO rowKeyColumnIO = new RowKeyColumnIO(cubeSegment);

        Dictionary<String> dict = rowKeyColumnIO.getDictionary(column);
        // in case that dict is null, just return values
        if (dict == null) return values;

        Collection<String> newValues = new LinkedList<String>();
        for (String value : values) {
            if (isInDictionary(dict, value)) {
                newValues.add(value);
            }
            else {
                try {
                    int id = dict.getIdFromValue(value, roundingFlag);
                    String newValue = dict.getValueFromId(id);
                    newValues.add(newValue);
                } catch (IllegalArgumentException ex) {
                }
            }
        }

        return newValues;
    }

    private static Collection<String> optimizeCompareTupleFilter(CubeSegment cubeSegment, TblColRef column, CompareTupleFilter comp) {
        Collection<String> newValues = comp.getValues();
        switch (comp.getOperator()) {
            case EQ:
            case IN:
                newValues = removeNonDictionaryValues(cubeSegment, column, comp.getValues());
                break;
            case LT:
            case LTE:
                newValues = roundDictionaryValues(cubeSegment, column, comp.getValues(), -1);
                break;
            case GT:
            case GTE:
                newValues = roundDictionaryValues(cubeSegment, column, comp.getValues(), 1);
                break;
            default:
                break;
        }

        return newValues;
    }

    public static Collection<String> doOptimization(CubeSegment cubeSegment, TblColRef column, TupleFilter filter) {
        if (filter instanceof CompareTupleFilter) {
            return optimizeCompareTupleFilter(cubeSegment, column, (CompareTupleFilter)filter);
        }

        return filter.getValues();
    }

    public static boolean isEmptyAnd(TupleFilter filter, Collection<String> values) {
        boolean isEmptyAnd = false;
        switch (filter.getOperator()) {
            case EQ:
            case IN:
            case LT:
            case LTE:
            case GT:
            case GTE:
                if (values == null || values.isEmpty()) {
                    isEmptyAnd = true;
                }
                break;
            default:
                break;
        }

        return isEmptyAnd;
    }
}
