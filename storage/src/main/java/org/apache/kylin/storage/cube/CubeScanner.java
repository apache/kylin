package org.apache.kylin.storage.cube;

import java.io.IOException;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.gridtable.CubeGridTable;
import org.apache.kylin.cube.gridtable.CuboidToGridTableMapping;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRange;
import org.apache.kylin.gridtable.GTScanRangePlanner;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.GTUtil;
import org.apache.kylin.gridtable.IGTCodeSystem;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.util.KryoUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class CubeScanner implements IGTScanner {

    private static final int MAX_SCAN_RANGES = 200;

    final CubeSegment cubeSeg;
    final GTInfo info;
    final byte[] trimmedInfoBytes;
    final List<GTScanRequest> scanRequests;
    final Scanner scanner;
    final Cuboid cuboid;

    public CubeScanner(CubeSegment cubeSeg, Cuboid cuboid, Set<TblColRef> dimensions, Set<TblColRef> groups, //
            Collection<FunctionDesc> metrics, TupleFilter filter, boolean allowPreAggregate) {
        this.cuboid = cuboid;
        this.cubeSeg = cubeSeg;
        this.info = CubeGridTable.newGTInfo(cubeSeg, cuboid.getId());

        CuboidToGridTableMapping mapping = cuboid.getCuboidToGridTableMapping();

        //replace the constant values in filter to dictionary codes 
        TupleFilter gtFilter = GTUtil.convertFilterColumnsAndConstants(filter, info, mapping.getCuboidDimensionsInGTOrder(), groups);

        ImmutableBitSet gtDimensions = makeGridTableColumns(mapping, dimensions);
        ImmutableBitSet gtAggrGroups = makeGridTableColumns(mapping, replaceDerivedColumns(groups, cubeSeg.getCubeDesc()));
        ImmutableBitSet gtAggrMetrics = makeGridTableColumns(mapping, metrics);
        String[] gtAggrFuncs = makeAggrFuncs(mapping, metrics);

        //TODO: should remove this in endpoint scenario
        GTScanRangePlanner scanRangePlanner = new GTScanRangePlanner(info);
        List<GTScanRange> scanRanges = scanRangePlanner.planScanRanges(gtFilter, MAX_SCAN_RANGES);

        scanRequests = Lists.newArrayListWithCapacity(scanRanges.size());

        IGTCodeSystem origin = info.trimCodeSystemForCoprocessor();
        trimmedInfoBytes = KryoUtils.serialize(info);
        info.setIGTCodeSystem(origin);
        GTInfo trimmedInfo = KryoUtils.deserialize(this.trimmedInfoBytes, GTInfo.class);

        for (GTScanRange range : scanRanges) {
            scanRequests.add(new GTScanRequest(trimmedInfo, range, gtDimensions, gtAggrGroups, gtAggrMetrics, gtAggrFuncs, gtFilter, allowPreAggregate));
        }

        scanner = new Scanner();
    }

    private Set<TblColRef> replaceDerivedColumns(Set<TblColRef> input, CubeDesc cubeDesc) {
        Set<TblColRef> ret = Sets.newHashSet();
        for (TblColRef col : input) {
            if (cubeDesc.isDerived(col)) {
                for (TblColRef host : cubeDesc.getHostInfo(col).columns) {
                    ret.add(host);
                }
            } else {
                ret.add(col);
            }
        }
        return ret;
    }

    private ImmutableBitSet makeGridTableColumns(CuboidToGridTableMapping mapping, Set<TblColRef> dimensions) {
        BitSet result = new BitSet();
        for (TblColRef dim : dimensions) {
            int idx = mapping.getIndexOf(dim);
            if (idx >= 0)
                result.set(idx);
        }
        return new ImmutableBitSet(result);
    }

    private ImmutableBitSet makeGridTableColumns(CuboidToGridTableMapping mapping, Collection<FunctionDesc> metrics) {
        BitSet result = new BitSet();
        for (FunctionDesc metric : metrics) {
            int idx = mapping.getIndexOf(metric);
            if (idx < 0)
                throw new IllegalStateException(metric + " not found in " + mapping);
            result.set(idx);
        }
        return new ImmutableBitSet(result);
    }

    private String[] makeAggrFuncs(final CuboidToGridTableMapping mapping, Collection<FunctionDesc> metrics) {

        //metrics are represented in ImmutableBitSet, which loses order information
        //sort the aggrFuns to align with metrics natural order 
        List<FunctionDesc> metricList = Lists.newArrayList(metrics);
        Collections.sort(metricList, new Comparator<FunctionDesc>() {
            @Override
            public int compare(FunctionDesc o1, FunctionDesc o2) {
                int a = mapping.getIndexOf(o1);
                int b = mapping.getIndexOf(o2);
                return a - b;
            }
        });

        String[] result = new String[metricList.size()];
        int i = 0;
        for (FunctionDesc metric : metricList) {
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

    static class RemoteGTRecordAdapter implements Iterable<GTRecord> {

        private final GTInfo info;
        private final Iterator<GTRecord> input;

        public RemoteGTRecordAdapter(GTInfo info, Iterator<GTRecord> input) {
            this.info = info;
            this.input = input;
        }

        @Override
        public Iterator<GTRecord> iterator() {
            return new Iterator<GTRecord>() {
                @Override
                public boolean hasNext() {
                    return input.hasNext();
                }

                @Override
                public GTRecord next() {
                    GTRecord x = input.next();
                    return new GTRecord(info, x.getInternal());
                }

                @Override
                public void remove() {

                }
            };
        }
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
                            CubeHBaseRPC rpc = new CubeHBaseEndpointRPC(cubeSeg, cuboid, info);
                            inputScanners[cur] = rpc.getGTScanner(scanRequests.get(cur));
                            curIterator = inputScanners[cur].iterator();
                            //curIterator = new RemoteGTRecordAdapter(info, inputScanners[cur].iterator()).iterator();
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
