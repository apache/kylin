package org.apache.kylin.storage.test;

import com.google.common.collect.ImmutableList;
import org.apache.kylin.metadata.filter.*;
import org.apache.kylin.metadata.model.*;
import org.apache.kylin.storage.tuple.TupleInfo;

import java.util.ArrayList;
import java.util.List;

/**
 */
public class StorageMockUtils {
    public static  TupleInfo newTupleInfo(List<TblColRef> groups, List<FunctionDesc> aggregations) {
        TupleInfo info = new TupleInfo();
        int idx = 0;

        for (TblColRef col : groups) {
            info.setField(col.getName(), col, idx++);
        }

        TableDesc sourceTable = groups.get(0).getColumnDesc().getTable();
        for (FunctionDesc func : aggregations) {
            TblColRef col = new TblColRef(func.newFakeRewriteColumn(sourceTable));
            info.setField(col.getName(), col, idx++);
        }

        return info;
    }

    public static  List<TblColRef> buildGroups() {
        List<TblColRef> groups = new ArrayList<TblColRef>();

        TableDesc t1 = TableDesc.mockup("DEFAULT.TEST_KYLIN_FACT");
        ColumnDesc c1 = ColumnDesc.mockup(t1, 2, "CAL_DT", "date");
        TblColRef cf1 = new TblColRef(c1);
        groups.add(cf1);

        TableDesc t2 = TableDesc.mockup("DEFAULT.TEST_CATEGORY_GROUPINGS");
        ColumnDesc c2 = ColumnDesc.mockup(t2, 14, "META_CATEG_NAME", "string");
        TblColRef cf2 = new TblColRef(c2);
        groups.add(cf2);

        return groups;
    }

    public static  List<FunctionDesc> buildAggregations() {
        List<FunctionDesc> functions = new ArrayList<FunctionDesc>();

        TableDesc t1 = TableDesc.mockup("DEFAULT.TEST_KYLIN_FACT");
        TblColRef priceCol = new TblColRef(ColumnDesc.mockup(t1, 7, "PRICE", "decimal(19,4)"));
        TblColRef sellerCol = new TblColRef(ColumnDesc.mockup(t1, 9, "SELLER_ID", "bigint"));

        FunctionDesc f1 = new FunctionDesc();
        f1.setExpression("SUM");
        ParameterDesc p1 = new ParameterDesc();
        p1.setType("column");
        p1.setValue("PRICE");
        p1.setColRefs(ImmutableList.of(priceCol));
        f1.setParameter(p1);
        functions.add(f1);

        FunctionDesc f2 = new FunctionDesc();
        f2.setExpression("COUNT_DISTINCT");
        ParameterDesc p2 = new ParameterDesc();
        p2.setType("column");
        p2.setValue("SELLER_ID");
        p2.setColRefs(ImmutableList.of(sellerCol));
        f2.setParameter(p2);
        functions.add(f2);

        return functions;
    }


    public static  CompareTupleFilter buildTs2010Filter(TblColRef column) {
        CompareTupleFilter compareFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.GT);
        ColumnTupleFilter columnFilter1 = new ColumnTupleFilter(column);
        compareFilter.addChild(columnFilter1);
        ConstantTupleFilter constantFilter1 = new ConstantTupleFilter("2010-01-01");
        compareFilter.addChild(constantFilter1);
        return compareFilter;
    }

    public static  CompareTupleFilter buildTs2011Filter(TblColRef column) {
        CompareTupleFilter compareFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.GT);
        ColumnTupleFilter columnFilter1 = new ColumnTupleFilter(column);
        compareFilter.addChild(columnFilter1);
        ConstantTupleFilter constantFilter1 = new ConstantTupleFilter("2011-01-01");
        compareFilter.addChild(constantFilter1);
        return compareFilter;
    }

    public static  CompareTupleFilter buildFilter1(TblColRef column) {
        CompareTupleFilter compareFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        ColumnTupleFilter columnFilter1 = new ColumnTupleFilter(column);
        compareFilter.addChild(columnFilter1);
        ConstantTupleFilter constantFilter1 = new ConstantTupleFilter("2012-05-23");
        compareFilter.addChild(constantFilter1);
        return compareFilter;
    }

    public static  CompareTupleFilter buildFilter2(TblColRef column) {
        CompareTupleFilter compareFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        ColumnTupleFilter columnFilter2 = new ColumnTupleFilter(column);
        compareFilter.addChild(columnFilter2);
        ConstantTupleFilter constantFilter2 = new ConstantTupleFilter("ClothinShoes & Accessories");
        compareFilter.addChild(constantFilter2);
        return compareFilter;
    }

    @SuppressWarnings("unused")
    public static  TupleFilter buildAndFilter(List<TblColRef> columns) {
        CompareTupleFilter compareFilter1 = buildFilter1(columns.get(0));
        CompareTupleFilter compareFilter2 = buildFilter2(columns.get(1));
        LogicalTupleFilter andFilter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
        andFilter.addChild(compareFilter1);
        andFilter.addChild(compareFilter2);
        return andFilter;
    }

    @SuppressWarnings("unused")
    public static  TupleFilter buildOrFilter(List<TblColRef> columns) {
        CompareTupleFilter compareFilter1 = buildFilter1(columns.get(0));
        CompareTupleFilter compareFilter2 = buildFilter2(columns.get(1));
        LogicalTupleFilter logicFilter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.OR);
        logicFilter.addChild(compareFilter1);
        logicFilter.addChild(compareFilter2);
        return logicFilter;
    }
}
