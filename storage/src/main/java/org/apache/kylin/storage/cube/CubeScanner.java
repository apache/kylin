package org.apache.kylin.storage.cube;

import java.io.IOException;
import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.gridtable.GTInfo;
import org.apache.kylin.storage.gridtable.GTRawScanner;
import org.apache.kylin.storage.gridtable.GTRecord;
import org.apache.kylin.storage.gridtable.GTScanRange;
import org.apache.kylin.storage.gridtable.GTScanRangePlanner;
import org.apache.kylin.storage.gridtable.GTScanRequest;
import org.apache.kylin.storage.gridtable.GTUtil;
import org.apache.kylin.storage.gridtable.IGTScanner;

import com.google.common.collect.Lists;

public class CubeScanner implements IGTScanner {

    private static final int MAX_SCAN_RANGES = 200;

    final CubeSegment cubeSeg;
    final GTInfo info;
    final CubeHBaseReadonlyStore store;
    final List<GTScanRequest> scanRequests;
    final Scanner scanner;

    public CubeScanner(CubeSegment cubeSeg, Cuboid cuboid, Set<TblColRef> dimensions, Set<TblColRef> groups, //
            Collection<FunctionDesc> metrics, TupleFilter filter) {
        this.cubeSeg = cubeSeg;
        this.info = CubeGridTable.newGTInfo(cubeSeg, cuboid.getId());
        this.store = new CubeHBaseReadonlyStore(info, cubeSeg, cuboid);

        CuboidToGridTableMapping mapping = new CuboidToGridTableMapping(cuboid);
        TupleFilter gtFilter = GTUtil.convertFilterColumnsAndConstants(filter, info, mapping.getCuboidDimensionsInGTOrder(), groups);
        BitSet gtDimensions = makeGridTableColumns(mapping, dimensions);
        BitSet gtAggrGroups = makeGridTableColumns(mapping, groups);
        BitSet gtAggrMetrics = makeGridTableColumns(mapping, metrics);
        String[] gtAggrFuncs = makeAggrFuncs(metrics);

        GTScanRangePlanner scanRangePlanner = new GTScanRangePlanner(info);
        List<GTScanRange> scanRanges = scanRangePlanner.planScanRanges(gtFilter, MAX_SCAN_RANGES);

        scanRequests = Lists.newArrayListWithCapacity(scanRanges.size());
        for (GTScanRange range : scanRanges) {
            scanRequests.add(new GTScanRequest(info, range, gtDimensions, gtAggrGroups, gtAggrMetrics, gtAggrFuncs, gtFilter));
        }
        
        scanner = new Scanner();
    }

    private BitSet makeGridTableColumns(CuboidToGridTableMapping mapping, Set<TblColRef> dimensions) {
        BitSet result = new BitSet();
        for (TblColRef dim : dimensions) {
            int idx = mapping.getIndexOf(dim);
            if (idx < 0)
                throw new IllegalStateException(dim + " not found in " + mapping);
            result.set(idx);
        }
        return result;
    }

    private BitSet makeGridTableColumns(CuboidToGridTableMapping mapping, Collection<FunctionDesc> metrics) {
        BitSet result = new BitSet();
        for (FunctionDesc metric : metrics) {
            int idx = mapping.getIndexOf(metric);
            if (idx < 0)
                throw new IllegalStateException(metric + " not found in " + mapping);
            result.set(idx);
        }
        return result;
    }

    private String[] makeAggrFuncs(Collection<FunctionDesc> metrics) {
        String[] result = new String[metrics.size()];
        int i = 0;
        for (FunctionDesc metric : metrics) {
            result[i++] = metric.getExpression();
        }
        return result;
    }

    @Override
    public Iterator<GTRecord> iterator() {
        return scanner.iterator();
    }

    @Override
    public void close() throws IOException {
        scanner.close();
    }

    @Override
    public GTInfo getInfo() {
        return info;
    }

    @Override
    public int getScannedRowCount() {
        return scanner.getScannedRowCount();
    }

    @Override
    public int getScannedRowBlockCount() {
        return scanner.getScannedRowBlockCount();
    }

    private class Scanner {
        final IGTScanner[] inputScanners = new IGTScanner[scanRequests.size()];
        int cur = 0;
        Iterator<GTRecord> curIterator = null;
        GTRecord next = null;

        public Iterator<GTRecord> iterator() {
            return new Iterator<GTRecord>() {

                @Override
                public boolean hasNext() {
                    if (next != null)
                        return true;
                    
                    if (curIterator == null) {
                        if (cur >= scanRequests.size())
                            return false;

                        try {
                            inputScanners[cur] = new GTRawScanner(info, store, scanRequests.get(cur));
                            curIterator = inputScanners[cur].iterator();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    
                    if (curIterator.hasNext() == false) {
                        curIterator = null;
                        cur++;
                        return hasNext();
                    }
                    
                    next = curIterator.next();
                    return true;
                }

                @Override
                public GTRecord next() {
                    // fetch next record
                    if (next == null) {
                        hasNext();
                        if (next == null)
                            throw new NoSuchElementException();
                    }

                    GTRecord result = next;
                    next = null;
                    return result;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        public void close() throws IOException {
            for (int i = 0; i < inputScanners.length; i++) {
                if (inputScanners[i] != null) {
                    inputScanners[i].close();
                }
            }
        }

        public int getScannedRowCount() {
            int result = 0;
            for (int i = 0; i < inputScanners.length; i++) {
                if (inputScanners[i] == null)
                    break;
                
                result += inputScanners[i].getScannedRowCount();
            }
            return result;
        }

        public int getScannedRowBlockCount() {
            int result = 0;
            for (int i = 0; i < inputScanners.length; i++) {
                if (inputScanners[i] == null)
                    break;
                
                result += inputScanners[i].getScannedRowBlockCount();
            }
            return result;
        }

    }
    
}
