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

package org.apache.kylin.storage.translate;

import java.util.List;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Array;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.kv.RowKeyColumnOrder;
import org.apache.kylin.cube.model.CubeDesc.DeriveInfo;
import org.apache.kylin.cube.model.CubeDesc.DeriveType;
import org.apache.kylin.dict.lookup.LookupStringTable;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.FilterCodeSystemFactory;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.IEvaluatableTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * @author yangli9
 */
public class DerivedFilterTranslator {

    private static final Logger logger = LoggerFactory.getLogger(DerivedFilterTranslator.class);

    public static Pair<TupleFilter, Boolean> translate(LookupStringTable lookup, DeriveInfo hostInfo, CompareTupleFilter compf) {

        TblColRef derivedCol = compf.getColumn();
        TblColRef[] hostCols = hostInfo.columns;
        TblColRef[] pkCols = hostInfo.join.getPrimaryKeyColumns();

        if (hostInfo.type == DeriveType.PK_FK) {
            assert hostCols.length == 1;
            CompareTupleFilter newComp = new CompareTupleFilter(compf.getOperator());
            newComp.addChild(new ColumnTupleFilter(hostCols[0]));
            newComp.addChild(new ConstantTupleFilter(compf.getValues()));
            return new Pair<TupleFilter, Boolean>(newComp, false);
        }

        assert hostInfo.type == DeriveType.LOOKUP;
        assert hostCols.length == pkCols.length;

        int di = derivedCol.getColumnDesc().getZeroBasedIndex();
        int[] pi = new int[pkCols.length];
        int hn = hostCols.length;
        for (int i = 0; i < hn; i++) {
            pi[i] = pkCols[i].getColumnDesc().getZeroBasedIndex();
        }

        Set<Array<String>> satisfyingHostRecords = Sets.newHashSet();
        SingleColumnTuple tuple = new SingleColumnTuple(derivedCol);
        for (String[] row : lookup.getAllRows()) {
            tuple.value = row[di];
            if (compf.evaluate(tuple, FilterCodeSystemFactory.getFilterCodeSystem(derivedCol.getColumnDesc().getType()))) {
                collect(row, pi, satisfyingHostRecords);
            }
        }

        TupleFilter translated;
        boolean loosened;
        if (satisfyingHostRecords.size() > KylinConfig.getInstanceFromEnv().getDerivedInThreshold()) {
            logger.info("Deciding to loosen filter on derived filter as host candidates number {} exceeds threshold {}", //
                    satisfyingHostRecords.size(), KylinConfig.getInstanceFromEnv().getDerivedInThreshold()
            );
            translated = buildRangeFilter(hostCols, satisfyingHostRecords);
            loosened = true;
        } else {
            translated = buildInFilter(hostCols, satisfyingHostRecords);
            loosened = false;
        }

        return new Pair<TupleFilter, Boolean>(translated, loosened);
    }

    private static void collect(String[] row, int[] pi, Set<Array<String>> satisfyingHostRecords) {
        // TODO when go beyond IN_THRESHOLD, only keep min/max is enough
        String[] rec = new String[pi.length];
        for (int i = 0; i < pi.length; i++) {
            rec[i] = row[pi[i]];
        }
        satisfyingHostRecords.add(new Array<String>(rec));
    }

    private static TupleFilter buildInFilter(TblColRef[] hostCols, Set<Array<String>> satisfyingHostRecords) {
        if (satisfyingHostRecords.size() == 0) {
            return ConstantTupleFilter.FALSE;
        }

        int hn = hostCols.length;
        if (hn == 1) {
            CompareTupleFilter in = new CompareTupleFilter(FilterOperatorEnum.IN);
            in.addChild(new ColumnTupleFilter(hostCols[0]));
            in.addChild(new ConstantTupleFilter(asValues(satisfyingHostRecords)));
            return in;
        } else {
            LogicalTupleFilter or = new LogicalTupleFilter(FilterOperatorEnum.OR);
            for (Array<String> rec : satisfyingHostRecords) {
                LogicalTupleFilter and = new LogicalTupleFilter(FilterOperatorEnum.AND);
                for (int i = 0; i < hn; i++) {
                    CompareTupleFilter eq = new CompareTupleFilter(FilterOperatorEnum.EQ);
                    eq.addChild(new ColumnTupleFilter(hostCols[i]));
                    eq.addChild(new ConstantTupleFilter(rec.data[i]));
                    and.addChild(eq);
                }
                or.addChild(and);
            }
            return or;
        }
    }

    private static List<String> asValues(Set<Array<String>> satisfyingHostRecords) {
        List<String> values = Lists.newArrayListWithCapacity(satisfyingHostRecords.size());
        for (Array<String> rec : satisfyingHostRecords) {
            values.add(rec.data[0]);
        }
        return values;
    }

    private static LogicalTupleFilter buildRangeFilter(TblColRef[] hostCols, Set<Array<String>> satisfyingHostRecords) {
        int hn = hostCols.length;
        String[] min = new String[hn];
        String[] max = new String[hn];
        findMinMax(satisfyingHostRecords, hostCols, min, max);
        LogicalTupleFilter and = new LogicalTupleFilter(FilterOperatorEnum.AND);
        for (int i = 0; i < hn; i++) {
            CompareTupleFilter compMin = new CompareTupleFilter(FilterOperatorEnum.GTE);
            compMin.addChild(new ColumnTupleFilter(hostCols[i]));
            compMin.addChild(new ConstantTupleFilter(min[i]));
            and.addChild(compMin);
            CompareTupleFilter compMax = new CompareTupleFilter(FilterOperatorEnum.LTE);
            compMax.addChild(new ColumnTupleFilter(hostCols[i]));
            compMax.addChild(new ConstantTupleFilter(max[i]));
            and.addChild(compMax);
        }
        return and;
    }

    private static void findMinMax(Set<Array<String>> satisfyingHostRecords, TblColRef[] hostCols, String[] min, String[] max) {

        RowKeyColumnOrder[] orders = new RowKeyColumnOrder[hostCols.length];
        for (int i = 0; i < hostCols.length; i++) {
            orders[i] = RowKeyColumnOrder.getInstance(hostCols[i].getType());
        }

        for (Array<String> rec : satisfyingHostRecords) {
            String[] row = rec.data;
            for (int i = 0; i < row.length; i++) {
                min[i] = orders[i].min(min[i], row[i]);
                max[i] = orders[i].max(max[i], row[i]);
            }
        }
    }

    private static class SingleColumnTuple implements IEvaluatableTuple {

        private TblColRef col;
        private String value;

        SingleColumnTuple(TblColRef col) {
            this.col = col;
        }

        @Override
        public Object getValue(TblColRef col) {
            if (this.col.equals(col))
                return value;
            else
                throw new IllegalArgumentException("unexpected column " + col);
        }

    }

}
