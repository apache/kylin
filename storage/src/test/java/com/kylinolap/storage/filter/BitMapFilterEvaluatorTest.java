package com.kylinolap.storage.filter;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.junit.Test;

import it.uniroma3.mat.extendedset.intset.ConciseSet;

import com.google.common.collect.Lists;
import com.kylinolap.dict.Dictionary;
import com.kylinolap.metadata.model.cube.TblColRef;
import com.kylinolap.metadata.model.schema.ColumnDesc;
import com.kylinolap.metadata.model.schema.TableDesc;
import com.kylinolap.storage.filter.BitMapFilterEvaluator.BitMapProvider;
import com.kylinolap.storage.filter.TupleFilter.FilterOperatorEnum;

public class BitMapFilterEvaluatorTest {

    static TblColRef colA;
    static TblColRef colB;

    static {
        TableDesc table = new TableDesc();
        table.setName("TABLE");

        ColumnDesc col = new ColumnDesc();
        col.setTable(table);
        col.setName("colA");
        colA = new TblColRef(col);
        
        col = new ColumnDesc();
        col.setTable(table);
        col.setName("colB");
        colB = new TblColRef(col);
    }

    static class MockBitMapProivder implements BitMapProvider {

        private static final int MAX_ID = 8;
        private static final int REC_COUNT = 10;

        @Override
        public ConciseSet getBitMap(TblColRef col, int valueId) {
            if (col.equals(colA) == false)
                return null;
            
            // i-th record has value ID i, and last record has value null
            ConciseSet bitMap = new ConciseSet();
            if (valueId < 0 || valueId > getMaxValueId(col)) // null
                bitMap.add(getRecordCount() - 1);
            else
                bitMap.add(valueId);

            return bitMap;
        }

        @Override
        public int getRecordCount() {
            return REC_COUNT;
        }

        @Override
        public int getMaxValueId(TblColRef col) {
            return MAX_ID;
        }
    }

    BitMapFilterEvaluator eval = new BitMapFilterEvaluator(new MockBitMapProivder());
    ArrayList<CompareTupleFilter> basicFilters = Lists.newArrayList();
    ArrayList<ConciseSet> basicResults = Lists.newArrayList();
    
    public BitMapFilterEvaluatorTest() {
        basicFilters.add(compare(colA, FilterOperatorEnum.ISNULL));
        basicResults.add(set(9));
        
        basicFilters.add(compare(colA, FilterOperatorEnum.ISNOTNULL));
        basicResults.add(set(0, 1, 2, 3, 4, 5, 6, 7, 8));
        
        basicFilters.add(compare(colA, FilterOperatorEnum.EQ, 0));
        basicResults.add(set(0));
        
        basicFilters.add(compare(colA, FilterOperatorEnum.NEQ, 0));
        basicResults.add(set(1, 2, 3, 4, 5, 6, 7, 8));
        
        basicFilters.add(compare(colA, FilterOperatorEnum.IN, 0, 5));
        basicResults.add(set(0, 5));
        
        basicFilters.add(compare(colA, FilterOperatorEnum.NOTIN, 0, 5));
        basicResults.add(set(1, 2, 3, 4, 6, 7, 8));
        
        basicFilters.add(compare(colA, FilterOperatorEnum.LT, 3));
        basicResults.add(set(0, 1, 2));
        
        basicFilters.add(compare(colA, FilterOperatorEnum.LTE, 3));
        basicResults.add(set(0, 1, 2, 3));
        
        basicFilters.add(compare(colA, FilterOperatorEnum.GT, 3));
        basicResults.add(set(4, 5, 6, 7, 8));
        
        basicFilters.add(compare(colA, FilterOperatorEnum.GTE, 3));
        basicResults.add(set(3, 4, 5, 6, 7, 8));
    }

    private ConciseSet set(int... ints) {
        ConciseSet set = new ConciseSet();
        for (int i : ints)
            set.add(i);
        return set;
    }

    @Test
    public void testBasics() {
        for (int i = 0; i < basicFilters.size(); i++) {
            assertEquals(basicResults.get(i), eval.evaluate(basicFilters.get(i)));
        }
    }
    
    @Test
    public void testLogicalAnd() {
        for (int i = 0; i < basicFilters.size(); i++) {
            for (int j = 0; j < basicFilters.size(); j++) {
                LogicalTupleFilter f = logical(FilterOperatorEnum.AND, basicFilters.get(i), basicFilters.get(j));
                ConciseSet r = basicResults.get(i).clone();
                r.retainAll(basicResults.get(j));
                assertEquals(r, eval.evaluate(f));
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
                assertEquals(r, eval.evaluate(f));
            }
        }
    }
    
    @Test
    public void testNotEvaluable() {
        CompareTupleFilter notEvaluable = compare(colB, FilterOperatorEnum.EQ, 0);
        assertEquals(null, eval.evaluate(notEvaluable));
        
        LogicalTupleFilter or = logical(FilterOperatorEnum.OR, basicFilters.get(1), notEvaluable);
        assertEquals(null, eval.evaluate(or));
        
        LogicalTupleFilter and = logical(FilterOperatorEnum.AND, basicFilters.get(1), notEvaluable);
        assertEquals(basicResults.get(1), eval.evaluate(and));
    }
    
    private CompareTupleFilter compare(TblColRef col, FilterOperatorEnum op, int... ids) {
        CompareTupleFilter filter = new CompareTupleFilter(op);
        filter.setNullString(idToStr(Dictionary.NULL_ID[1]));
        filter.addChild(columnFilter(col));
        for (int i : ids) {
            filter.addChild(constFilter(i));
        }
        return filter;
    }
    
    private LogicalTupleFilter logical(FilterOperatorEnum op, TupleFilter... filters) {
        LogicalTupleFilter filter = new LogicalTupleFilter(op);
        for (TupleFilter f : filters)
            filter.addChild(f);
        return filter;
    }
    
    private ColumnTupleFilter columnFilter(TblColRef col) {
        return new ColumnTupleFilter(col);
    }

    private ConstantTupleFilter constFilter(int id) {
        return new ConstantTupleFilter(idToStr(id));
    }
    
    private String idToStr(int id) {
        byte[] bytes = new byte[] { (byte) id };
        return Dictionary.dictIdToString(bytes, 0, bytes.length);
    }
}
