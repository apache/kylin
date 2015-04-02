package org.apache.kylin.storage.cube;

import java.io.IOException;
import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.gridtable.GTInfo;
import org.apache.kylin.storage.gridtable.GTRecord;
import org.apache.kylin.storage.gridtable.GTScanRange;
import org.apache.kylin.storage.gridtable.GTScanRangePlanner;
import org.apache.kylin.storage.gridtable.GTScanRequest;
import org.apache.kylin.storage.gridtable.GTUtil;
import org.apache.kylin.storage.gridtable.IGTScanner;

import com.google.common.collect.Lists;

public class CubeScanner {

    private static final int MAX_SCAN_RANGES = 200;
    
    final GTInfo info;
    final List<GTScanRequest> scanRequests;

    public CubeScanner(CubeSegment cubeSeg, Set<TblColRef> dimensions, Set<TblColRef> groups, //
            Collection<FunctionDesc> metrics, TupleFilter filter, StorageContext context) {
        Cuboid cuboid = context.getCuboid();
        info = CubeGridTable.newGTInfo(cubeSeg, cuboid.getId());
        
        TupleFilter gtFilter = GTUtil.convertFilterColumnsAndConstants(filter, info, cuboid.getColumns(), groups);
        BitSet gtDimensions = makeGridTableColumns(cuboid, dimensions);
        BitSet gtAggrGroups = makeGridTableColumns(cuboid, groups);
        BitSet gtAggrMetrics = makeGridTableColumns(cubeSeg.getCubeDesc(), cuboid, metrics);
        String[] gtAggrFuncs = makeAggrFuncs(metrics);
        
        GTScanRangePlanner scanRangePlanner = new GTScanRangePlanner(info);
        List<GTScanRange> scanRanges = scanRangePlanner.planScanRanges(gtFilter, MAX_SCAN_RANGES);
        
        scanRequests = Lists.newArrayListWithCapacity(scanRanges.size());
        for (GTScanRange range : scanRanges) {
            scanRequests.add(new GTScanRequest(info, range, gtDimensions, gtAggrGroups, gtAggrMetrics, gtAggrFuncs, gtFilter));
        }
    }
    
    private BitSet makeGridTableColumns(Cuboid cuboid, Set<TblColRef> dimensions) {
        BitSet result = new BitSet();
        List<TblColRef> dimCols = cuboid.getColumns();
        for (int i = 0; i < dimCols.size(); i++) {
            if (dimensions.contains(dimCols.get(i))) {
                result.set(i);
            }
        }
        return result;
    }

    private BitSet makeGridTableColumns(CubeDesc cubeDesc, Cuboid cuboid, Collection<FunctionDesc> metrics) {
        BitSet result = new BitSet();
        int metricsIndexStart = cuboid.getColumns().size();
        for (FunctionDesc metric : metrics) {
            int index = cubeDesc.getMeasures().indexOf(metric);
            if (index < 0)
                throw new IllegalStateException(metric + " not found in " + cubeDesc);
            
            result.set(metricsIndexStart + index);
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

    public IGTScanner scan() {
        return new Scanner();
    }
    
    private class Scanner implements IGTScanner {
        int curRequestIndex = 0;
        
        // TODO hbase metrics
        int scannedRowCount = 0;
        int scannedRowBlockCount = 0;

        @Override
        public Iterator<GTRecord> iterator() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public void close() throws IOException {
            // TODO Auto-generated method stub
            
        }

        @Override
        public GTInfo getInfo() {
            return info;
        }

        @Override
        public int getScannedRowCount() {
            return scannedRowCount;
        }

        @Override
        public int getScannedRowBlockCount() {
            return scannedRowBlockCount;
        }
        
    }

}
