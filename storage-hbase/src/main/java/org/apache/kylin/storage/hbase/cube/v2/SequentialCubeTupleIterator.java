package org.apache.kylin.storage.hbase.cube.v2;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.tuple.Tuple;
import org.apache.kylin.storage.tuple.TupleInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SequentialCubeTupleIterator implements ITupleIterator {

    private static final Logger logger = LoggerFactory.getLogger(SequentialCubeTupleIterator.class);

    protected final Cuboid cuboid;
    protected final Set<TblColRef> selectedDimensions;
    protected final Set<FunctionDesc> selectedMetrics;
    protected final TupleInfo tupleInfo;
    protected final Tuple tuple;
    protected final Iterator<CubeSegmentScanner> scannerIterator;
    protected final StorageContext context;

    protected CubeSegmentScanner curScanner;
    protected Iterator<GTRecord> curRecordIterator;
    protected CubeTupleConverter curTupleConverter;
    protected Tuple next;

    private int scanCount;
    private int scanCountDelta;

    public SequentialCubeTupleIterator(List<CubeSegmentScanner> scanners, Cuboid cuboid, Set<TblColRef> selectedDimensions, //
            Set<FunctionDesc> selectedMetrics, TupleInfo returnTupleInfo, StorageContext context) {
        this.cuboid = cuboid;
        this.selectedDimensions = selectedDimensions;
        this.selectedMetrics = selectedMetrics;
        this.tupleInfo = returnTupleInfo;
        this.tuple = new Tuple(returnTupleInfo);
        this.scannerIterator = scanners.iterator();
        this.context = context;
    }

    @Override
    public boolean hasNext() {
        if (next != null)
            return true;

        if (curScanner == null) {
            if (scannerIterator.hasNext()) {
                curScanner = scannerIterator.next();
                curRecordIterator = curScanner.iterator();
                if (curRecordIterator.hasNext()) {
                    //if the segment does not has any tuples, don't bother to create a converter
                    curTupleConverter = new CubeTupleConverter(curScanner.cubeSeg, cuboid, selectedDimensions, selectedMetrics, tupleInfo, null);
                }
            } else {
                return false;
            }
        }

        if (curRecordIterator.hasNext()) {
            curTupleConverter.translateResult(curRecordIterator.next(), tuple);
            next = tuple;
            return true;
        } else {
            close(curScanner);
            curScanner = null;
            curRecordIterator = null;
            curTupleConverter = null;
            return hasNext();
        }
    }
    

    @Override
    public ITuple next() {
        // fetch next record
        if (next == null) {
            hasNext();
            if (next == null)
                throw new NoSuchElementException();
        }

        scanCount++;
        if (++scanCountDelta >= 1000)
            flushScanCountDelta();

        ITuple result = next;
        next = null;
        return result;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        // hasNext() loop may exit because of limit, threshold, etc.
        // close all the remaining segmentIterator
        flushScanCountDelta();

        if (curScanner != null)
            close(curScanner);

        while (scannerIterator.hasNext()) {
            close(scannerIterator.next());
        }
    }

    protected void close(CubeSegmentScanner scanner) {
        try {
            scanner.close();
        } catch (IOException e) {
            logger.error("Exception when close CubeScanner", e);
        }
    }

    public int getScanCount() {
        return scanCount;
    }

    private void flushScanCountDelta() {
        context.increaseTotalScanCount(scanCountDelta);
        scanCountDelta = 0;
    }

}
