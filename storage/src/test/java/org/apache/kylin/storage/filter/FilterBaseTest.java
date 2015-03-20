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

package org.apache.kylin.storage.filter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.apache.kylin.metadata.filter.*;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.storage.tuple.Tuple;
import org.apache.kylin.storage.tuple.TupleInfo;

/**
 * @author xjiang
 * 
 */
public class FilterBaseTest {
    
    static final IFilterCodeSystem CS = StringCodeSystem.INSTANCE;

    protected List<TblColRef> buildGroups() {
        List<TblColRef> groups = new ArrayList<TblColRef>();

        TableDesc t1 = new TableDesc();
        t1.setName("TEST_KYLIN_FACT");
        t1.setDatabase("DEFAULT");
        ColumnDesc c1 = new ColumnDesc();
        c1.setName("CAL_DT");
        c1.setDatatype("String");
        c1.setTable(t1);
        TblColRef cf1 = new TblColRef(c1);
        groups.add(cf1);

        TableDesc t2 = new TableDesc();
        t2.setName("TEST_CATEGORY_GROUPINGS");
        t2.setDatabase("DEFAULT");
        ColumnDesc c2 = new ColumnDesc();
        c2.setName("META_CATEG_NAME");
        c1.setDatatype("String");
        c2.setTable(t2);
        TblColRef cf2 = new TblColRef(c2);
        groups.add(cf2);

        return groups;
    }

    protected CompareTupleFilter buildCompareFilter(List<TblColRef> groups, int index) {
        TblColRef column = groups.get(index);
        CompareTupleFilter compareFilter = new CompareTupleFilter(FilterOperatorEnum.EQ);
        ColumnTupleFilter columnFilter = new ColumnTupleFilter(column);
        compareFilter.addChild(columnFilter);
        ConstantTupleFilter constantFilter = null;
        if (index == 0) {
            constantFilter = new ConstantTupleFilter("2013-03-10");
        } else if (index == 1) {
            constantFilter = new ConstantTupleFilter("ClothinShoes & Accessories");
        }
        compareFilter.addChild(constantFilter);
        return compareFilter;
    }

    protected TupleFilter buildAndFilter(List<TblColRef> columns) {
        CompareTupleFilter compareFilter1 = buildCompareFilter(columns, 0);
        CompareTupleFilter compareFilter2 = buildCompareFilter(columns, 1);
        LogicalTupleFilter andFilter = new LogicalTupleFilter(FilterOperatorEnum.AND);
        andFilter.addChild(compareFilter1);
        andFilter.addChild(compareFilter2);
        return andFilter;
    }

    protected TupleFilter buildOrFilter(List<TblColRef> columns) {
        CompareTupleFilter compareFilter1 = buildCompareFilter(columns, 0);
        CompareTupleFilter compareFilter2 = buildCompareFilter(columns, 1);
        LogicalTupleFilter logicFilter = new LogicalTupleFilter(FilterOperatorEnum.OR);
        logicFilter.addChild(compareFilter1);
        logicFilter.addChild(compareFilter2);
        return logicFilter;
    }

    protected CaseTupleFilter buildCaseFilter(List<TblColRef> groups) {
        CaseTupleFilter caseFilter = new CaseTupleFilter();

        TupleFilter when0 = buildAndFilter(groups);
        caseFilter.addChild(when0);
        TupleFilter then0 = new ConstantTupleFilter("0");
        caseFilter.addChild(then0);

        TupleFilter when1 = buildCompareFilter(groups, 0);
        caseFilter.addChild(when1);
        TupleFilter then1 = new ConstantTupleFilter("1");
        caseFilter.addChild(then1);

        TupleFilter when2 = buildCompareFilter(groups, 1);
        caseFilter.addChild(when2);
        TupleFilter then2 = new ConstantTupleFilter("2");
        caseFilter.addChild(then2);

        TupleFilter when3 = buildOrFilter(groups);
        caseFilter.addChild(when3);
        TupleFilter then3 = new ConstantTupleFilter("3");
        caseFilter.addChild(then3);

        TupleFilter else4 = new ConstantTupleFilter("4");
        caseFilter.addChild(else4);

        return caseFilter;
    }

    protected CompareTupleFilter buildCompareCaseFilter(List<TblColRef> groups, String constValue) {
        CompareTupleFilter compareFilter = new CompareTupleFilter(FilterOperatorEnum.EQ);
        CaseTupleFilter caseFilter = buildCaseFilter(groups);
        compareFilter.addChild(caseFilter);
        ConstantTupleFilter constantFilter = new ConstantTupleFilter(constValue);
        compareFilter.addChild(constantFilter);
        return compareFilter;
    }

    protected CompareTupleFilter buildCompareDynamicFilter(List<TblColRef> groups) {
        CompareTupleFilter compareFilter = new CompareTupleFilter(FilterOperatorEnum.EQ);
        compareFilter.addChild(new ColumnTupleFilter(groups.get(0)));
        compareFilter.addChild(new DynamicTupleFilter("?0"));
        compareFilter.bindVariable("?0", "abc");
        return compareFilter;
    }

    protected void compareFilter(TupleFilter f1, TupleFilter f2) {
        if (f1 == null && f2 == null) {
            return;
        }

        if (f1 == null || f2 == null) {
            throw new IllegalStateException("f1=" + f1 + ", f2=" + f2);
        }

        String str1 = f1.toString();
        System.out.println("f1=" + str1);
        String str2 = f2.toString();
        System.out.println("f2=" + str2);
        if (!str1.equals(str2)) {
            throw new IllegalStateException("f1=" + str1 + ", f2=" + str2);
        }

        int s1 = f1.getChildren().size();
        int s2 = f2.getChildren().size();
        if (s1 != s2) {
            throw new IllegalStateException("f1=" + str1 + ", f2=" + str2 + " has different children: " + s1 + " vs. " + s2);
        }

        for (int i = 0; i < s1; i++) {
            compareFilter(f1.getChildren().get(i), f2.getChildren().get(i));
        }
    }

    private static String[][] SAMPLE_DATA = new String[][] { { "2013-03-10", "2012-01-12", "2014-03-10" }, { "ClothinShoes & Accessories", "ABIN", "FP-GTC", "FP-NON-GTC" } };

    protected Collection<Tuple> generateTuple(int number, List<TblColRef> columns, int[] matches) {

        Collection<Tuple> tuples = new ArrayList<Tuple>(number);
        TupleInfo info = new TupleInfo();
        for (int i = 0; i < columns.size(); i++) {
            TblColRef column = columns.get(i);
            info.setField(column.getName(), column, column.getDatatype(), i);
        }

        int allMatches = 0;
        Random rand = new Random();
        for (int i = 0; i < number; i++) {
            Tuple t = new Tuple(info);
            boolean isFullMatch = true;
            for (int k = 0; k < columns.size(); k++) {
                TblColRef column = columns.get(k);
                int index = Math.abs(rand.nextInt()) % SAMPLE_DATA[k].length;
                t.setDimensionValue(column.getName(), SAMPLE_DATA[k][index]);
                if (index == 0) {
                    matches[k]++;
                } else {
                    isFullMatch = false;
                }
            }
            if (isFullMatch) {
                allMatches++;
            }
            tuples.add(t);
        }
        matches[2] = allMatches;
        return tuples;
    }

    protected int evaluateTuples(Collection<Tuple> tuples, TupleFilter filter) {
        int match = 0;
        for (Tuple t : tuples) {
            if (filter.evaluate(t, CS)) {
                match++;
            }
        }
        return match;
    }

}
