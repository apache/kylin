package org.apache.kylin.storage.gridtable;

import it.uniroma3.mat.extendedset.intset.ConciseSet;

import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;

/**
 * A thread-safe inverted index of row blocks in memory.
 * 
 * Note function not() must return all blocks, because index only know what block contains a value,
 * but not sure what block does not contain a value.
 * 
 * @author yangli9
 */
public class GTInvertedIndex {

    private final GTInfo info;
    private final BitSet colPreferIndex;
    private final BitSet colBlocks;
    private final GTInvertedIndexOfColumn[] index; // for each column

    private volatile int nIndexedBlocks;

    public GTInvertedIndex(GTInfo info) {
        this.info = info;
        this.colPreferIndex = info.colPreferIndex;
        this.colBlocks = info.selectColumnBlocks(colPreferIndex);

        index = new GTInvertedIndexOfColumn[info.nColumns];
        for (int i = colPreferIndex.nextSetBit(0); i >= 0; i = colPreferIndex.nextSetBit(i + 1)) {
            index[i] = new GTInvertedIndexOfColumn(info.codeSystem.getFilterCodeSystem());
        }
    }

    public void add(GTRowBlock block) {

        @SuppressWarnings("unchecked")
        Set<ByteArray>[] distinctValues = new Set[info.nColumns];
        for (int i = colPreferIndex.nextSetBit(0); i >= 0; i = colPreferIndex.nextSetBit(i + 1)) {
            distinctValues[i] = new HashSet<ByteArray>();
        }

        GTRowBlock.Reader reader = block.getReader(colBlocks);
        GTRecord record = new GTRecord(info);
        while (reader.hasNext()) {
            reader.fetchNext(record);
            for (int i = colPreferIndex.nextSetBit(0); i >= 0; i = colPreferIndex.nextSetBit(i + 1)) {
                distinctValues[i].add(record.get(i));
            }
        }

        for (int i = colPreferIndex.nextSetBit(0); i >= 0; i = colPreferIndex.nextSetBit(i + 1)) {
            index[i].add(distinctValues[i], block.getSequenceId());
        }

        nIndexedBlocks = Math.max(nIndexedBlocks, block.seqId + 1);
    }

    public ConciseSet filter(TupleFilter filter) {
        return filter(filter, nIndexedBlocks);
    }
    
    public ConciseSet filter(TupleFilter filter, int totalBlocks) {
        // number of indexed blocks may increase as we do evaluation
        int indexedBlocks = nIndexedBlocks;

        Evaluator evaluator = new Evaluator(indexedBlocks);
        ConciseSet r = evaluator.evaluate(filter);

        // add blocks that have not been indexed
        for (int i = indexedBlocks; i < totalBlocks; i++) {
            r.add(i);
        }

        return r;
    }

    private class Evaluator {
        private int indexedBlocks;

        Evaluator(int indexedBlocks) {
            this.indexedBlocks = indexedBlocks;
        }

        public ConciseSet evaluate(TupleFilter filter) {
            if (filter == null) {
                return all();
            }

            if (filter instanceof LogicalTupleFilter)
                return evalLogical((LogicalTupleFilter) filter);

            if (filter instanceof CompareTupleFilter)
                return evalCompare((CompareTupleFilter) filter);

            // unable to evaluate
            return all();
        }

        @SuppressWarnings("unchecked")
        private ConciseSet evalCompare(CompareTupleFilter filter) {
            int col = col(filter);
            if (index[col] == null)
                return all();
            
            switch (filter.getOperator()) {
            case ISNULL:
                return index[col].getNull();
            case ISNOTNULL:
                return all();
            case EQ:
                return index[col].getEquals((ByteArray) filter.getFirstValue());
            case NEQ:
                return all();
            case IN:
                return index[col].getIn((Iterable<ByteArray>) filter.getValues());
            case NOTIN:
                return all();
            case LT:
                return index[col].getRange(null, false, (ByteArray) filter.getFirstValue(), false);
            case LTE:
                return index[col].getRange(null, false, (ByteArray) filter.getFirstValue(), true);
            case GT:
                return index[col].getRange((ByteArray) filter.getFirstValue(), false, null, false);
            case GTE:
                return index[col].getRange((ByteArray) filter.getFirstValue(), true, null, false);
            default:
                throw new IllegalStateException("Unsupported operator " + filter.getOperator());
            }
        }

        private ConciseSet evalLogical(LogicalTupleFilter filter) {
            List<? extends TupleFilter> children = filter.getChildren();

            switch (filter.getOperator()) {
            case AND:
                return evalLogicalAnd(children);
            case OR:
                return evalLogicalOr(children);
            case NOT:
                return evalLogicalNot(children);
            default:
                throw new IllegalStateException("Unsupported operator " + filter.getOperator());
            }
        }

        private ConciseSet evalLogicalAnd(List<? extends TupleFilter> children) {
            ConciseSet set = all();

            for (TupleFilter c : children) {
                ConciseSet t = evaluate(c);
                if (t == null)
                    continue; // because it's AND

                set.retainAll(t);
            }
            return set;
        }

        private ConciseSet evalLogicalOr(List<? extends TupleFilter> children) {
            ConciseSet set = new ConciseSet();

            for (TupleFilter c : children) {
                ConciseSet t = evaluate(c);
                if (t == null)
                    return null; // because it's OR

                set.addAll(t);
            }
            return set;
        }

        private ConciseSet evalLogicalNot(List<? extends TupleFilter> children) {
            return all();
        }

        private ConciseSet all() {
            return not(new ConciseSet());
        }
        
        private ConciseSet not(ConciseSet set) {
            set.add(indexedBlocks);
            set.complement();
            return set;
        }

        private int col(CompareTupleFilter filter) {
            return filter.getColumn().getColumn().getZeroBasedIndex();
        }

    }

}
