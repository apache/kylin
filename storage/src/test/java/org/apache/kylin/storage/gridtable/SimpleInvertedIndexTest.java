package org.apache.kylin.storage.gridtable;

import static org.junit.Assert.*;
import it.uniroma3.mat.extendedset.intset.ConciseSet;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.serializer.StringSerializer;
import org.junit.Test;

import com.google.common.collect.Lists;

public class SimpleInvertedIndexTest {

    GTInfo info;
    GTInvertedIndex index;
    ArrayList<CompareTupleFilter> basicFilters = Lists.newArrayList();
    ArrayList<ConciseSet> basicResults = Lists.newArrayList();

    public SimpleInvertedIndexTest() {
        
        info = SimpleGridTableTest.advancedInfo();
        TblColRef colA = info.colRef(0);
        
        // block i contains value "i", the last is NULL
        index = new GTInvertedIndex(info);
        GTRowBlock mockBlock = GTRowBlock.allocate(info);
        GTRowBlock.Writer writer = mockBlock.getWriter();
        GTRecord record = new GTRecord(info);
        for (int i = 0; i < 10; i++) {
            record.setValues(i < 9 ? "" + i : null, "", "", new LongWritable(0), new BigDecimal(0));
            for (int j = 0; j < info.getRowBlockSize(); j++) {
                writer.append(record);
            }
            writer.readyForFlush();
            index.add(mockBlock);
            
            writer.clearForNext();
        }
        
        basicFilters.add(compare(colA, FilterOperatorEnum.ISNULL));
        basicResults.add(set(9));

        basicFilters.add(compare(colA, FilterOperatorEnum.ISNOTNULL));
        basicResults.add(set(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));

        basicFilters.add(compare(colA, FilterOperatorEnum.EQ, 0));
        basicResults.add(set(0));

        basicFilters.add(compare(colA, FilterOperatorEnum.NEQ, 0));
        basicResults.add(set(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));

        basicFilters.add(compare(colA, FilterOperatorEnum.IN, 0, 5));
        basicResults.add(set(0, 5));

        basicFilters.add(compare(colA, FilterOperatorEnum.NOTIN, 0, 5));
        basicResults.add(set(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));

        basicFilters.add(compare(colA, FilterOperatorEnum.LT, 3));
        basicResults.add(set(0, 1, 2));

        basicFilters.add(compare(colA, FilterOperatorEnum.LTE, 3));
        basicResults.add(set(0, 1, 2, 3));

        basicFilters.add(compare(colA, FilterOperatorEnum.GT, 3));
        basicResults.add(set(4, 5, 6, 7, 8));

        basicFilters.add(compare(colA, FilterOperatorEnum.GTE, 3));
        basicResults.add(set(3, 4, 5, 6, 7, 8));
    }

    @Test
    public void testBasics() {
        for (int i = 0; i < basicFilters.size(); i++) {
            assertEquals(basicResults.get(i), index.filter(basicFilters.get(i)));
        }
    }

    @Test
    public void testLogicalAnd() {
        for (int i = 0; i < basicFilters.size(); i++) {
            for (int j = 0; j < basicFilters.size(); j++) {
                LogicalTupleFilter f = logical(FilterOperatorEnum.AND, basicFilters.get(i), basicFilters.get(j));
                ConciseSet r = basicResults.get(i).clone();
                r.retainAll(basicResults.get(j));
                assertEquals(r, index.filter(f));
            }
        }
    }

    @Test
    public void testLogicalOr() {
        for (int i = 0; i < basicFilters.size(); i++) {
            for (int j = 0; j < basicFilters.size(); j++) {
                LogicalTupleFilter f = logical(FilterOperatorEnum.OR, basicFilters.get(i), basicFilters.get(j));
                ConciseSet r = basicResults.get(i).clone();
                r.addAll(basicResults.get(j));
                assertEquals(r, index.filter(f));
            }
        }
    }

    @Test
    public void testNotEvaluable() {
        ConciseSet all = set(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        
        CompareTupleFilter notEvaluable = compare(info.colRef(1), FilterOperatorEnum.EQ, 0);
        assertEquals(all, index.filter(notEvaluable));

        LogicalTupleFilter or = logical(FilterOperatorEnum.OR, basicFilters.get(0), notEvaluable);
        assertEquals(all, index.filter(or));

        LogicalTupleFilter and = logical(FilterOperatorEnum.AND, basicFilters.get(0), notEvaluable);
        assertEquals(basicResults.get(0), index.filter(and));
    }

    public static CompareTupleFilter compare(TblColRef col, TupleFilter.FilterOperatorEnum op, int... ids) {
        CompareTupleFilter filter = new CompareTupleFilter(op);
        filter.addChild(columnFilter(col));
        for (int i : ids) {
            filter.addChild(constFilter(i));
        }
        return filter;
    }

    public static LogicalTupleFilter logical(TupleFilter.FilterOperatorEnum op, TupleFilter... filters) {
        LogicalTupleFilter filter = new LogicalTupleFilter(op);
        for (TupleFilter f : filters)
            filter.addChild(f);
        return filter;
    }

    public static ColumnTupleFilter columnFilter(TblColRef col) {
        return new ColumnTupleFilter(col);
    }

    public static ConstantTupleFilter constFilter(int id) {
        byte[] space = new byte[10];
        ByteBuffer buf = ByteBuffer.wrap(space);
        StringSerializer stringSerializer = new StringSerializer();
        stringSerializer.serialize("" + id, buf);
        ByteArray data = new ByteArray(buf.array(), buf.arrayOffset(), buf.position());
        return new ConstantTupleFilter(data);
    }

    public static ConciseSet set(int... ints) {
        ConciseSet set = new ConciseSet();
        for (int i : ints)
            set.add(i);
        return set;
    }


}
