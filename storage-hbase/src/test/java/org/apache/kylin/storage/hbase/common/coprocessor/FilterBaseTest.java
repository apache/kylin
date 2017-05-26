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

package org.apache.kylin.storage.hbase.common.coprocessor;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.filter.CaseTupleFilter;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.DynamicTupleFilter;
import org.apache.kylin.metadata.filter.IFilterCodeSystem;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.StringCodeSystem;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.Tuple;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.google.common.collect.Lists;

/**
 * @author xjiang
 *
 */
public class FilterBaseTest extends LocalFileMetadataTestCase {

    @BeforeClass
    public static void setUp() throws Exception {
        staticCreateTestMetadata();
    }

    @AfterClass
    public static void after() throws Exception {
        cleanAfterClass();
    }

    @SuppressWarnings("rawtypes")
    static final IFilterCodeSystem CS = StringCodeSystem.INSTANCE;

    protected List<TblColRef> buildGroups() {
        List<TblColRef> groups = new ArrayList<TblColRef>();

        TableDesc t1 = TableDesc.mockup("DEFAULT.TEST_KYLIN_FACT");
        TblColRef c1 = TblColRef.mockup(t1, 2, "CAL_DT", "string");
        groups.add(c1);

        TableDesc t2 = TableDesc.mockup("DEFAULT.TEST_CATEGORY_GROUPINGS");
        TblColRef c2 = TblColRef.mockup(t2, 14, "META_CATEG_NAME", "string");
        groups.add(c2);

        return groups;
    }

    protected CompareTupleFilter buildEQCompareFilter(List<TblColRef> groups, int index) {
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

    protected CompareTupleFilter buildINCompareFilter(TblColRef dateColumn) throws ParseException {
        CompareTupleFilter compareFilter = new CompareTupleFilter(FilterOperatorEnum.IN);
        ColumnTupleFilter columnFilter = new ColumnTupleFilter(dateColumn);
        compareFilter.addChild(columnFilter);

        List<String> inValues = Lists.newArrayList();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date startDate = simpleDateFormat.parse("1970-01-01");
        Date endDate = simpleDateFormat.parse("2100-01-01");
        Calendar start = Calendar.getInstance();
        start.setTime(startDate);
        Calendar end = Calendar.getInstance();
        end.setTime(endDate);
        for (Date date = start.getTime(); start.before(end); start.add(Calendar.DATE, 1), date = start.getTime()) {
            inValues.add(simpleDateFormat.format(date));
        }

        ConstantTupleFilter constantFilter = new ConstantTupleFilter(inValues);
        compareFilter.addChild(constantFilter);
        return compareFilter;
    }

    protected TupleFilter buildAndFilter(List<TblColRef> columns) {
        CompareTupleFilter compareFilter1 = buildEQCompareFilter(columns, 0);
        CompareTupleFilter compareFilter2 = buildEQCompareFilter(columns, 1);
        LogicalTupleFilter andFilter = new LogicalTupleFilter(FilterOperatorEnum.AND);
        andFilter.addChild(compareFilter1);
        andFilter.addChild(compareFilter2);
        return andFilter;
    }

    protected TupleFilter buildOrFilter(List<TblColRef> columns) {
        CompareTupleFilter compareFilter1 = buildEQCompareFilter(columns, 0);
        CompareTupleFilter compareFilter2 = buildEQCompareFilter(columns, 1);
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

        TupleFilter when1 = buildEQCompareFilter(groups, 0);
        caseFilter.addChild(when1);
        TupleFilter then1 = new ConstantTupleFilter("1");
        caseFilter.addChild(then1);

        TupleFilter when2 = buildEQCompareFilter(groups, 1);
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

    protected CompareTupleFilter buildCompareDynamicFilter(List<TblColRef> groups, FilterOperatorEnum operator) {
        CompareTupleFilter compareFilter = new CompareTupleFilter(operator);
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
        //System.out.println("f1=" + str1);
        String str2 = f2.toString();
        //System.out.println("f2=" + str2);
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
            info.setField(column.getName(), column, i);
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
