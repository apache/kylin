package org.apache.kylin.storage.cube;

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
import org.apache.kylin.storage.tuple.Tuple;
import org.apache.kylin.storage.tuple.TupleInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SequentialCubeTupleIterator implements ITupleIterator {

    private static final Logger logger = LoggerFactory.getLogger(SequentialCubeTupleIterator.class);

    private final Cuboid cuboid;
    private final Set<TblColRef> selectedDimensions;
    private final Set<FunctionDesc> selectedMetrics;
    private final TupleInfo tupleInfo;
    private final Tuple tuple;
    private final Iterator<CubeScanner> scannerIterator;
    
    private CubeScanner curScanner;
    private Iterator<GTRecord> curRecordIterator;
    private CubeTupleConverter curTupleConverter;
    private Tuple next;

    public SequentialCubeTupleIterator(List<CubeScanner> scanners, Cuboid cuboid, Set<TblColRef> selectedDimensions, //
            Set<FunctionDesc> selectedMetrics, TupleInfo returnTupleInfo) {
        this.cuboid = cuboid;
        this.selectedDimensions = selectedDimensions;
        this.selectedMetrics = selectedMetrics;
        this.tupleInfo = returnTupleInfo;
        this.tuple = new Tuple(returnTupleInfo);
        this.scannerIterator = scanners.iterator();
    }

    @Override
    public boolean hasNext() {
        if (next != null)
            return true;
        
        if (curScanner == null) {
            if (scannerIterator.hasNext()) {
                curScanner = scannerIterator.next();
                curRecordIterator = curScanner.iterator();
                curTupleConverter = new CubeTupleConverter(curScanner.cubeSeg, cuboid, selectedDimensions, selectedMetrics, tupleInfo);
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
        if (curScanner != null)
            close(curScanner);
        
        while (scannerIterator.hasNext()) {
            close(scannerIterator.next());
        }
    }
    
    private void close(CubeScanner scanner) {
        try {
            scanner.close();
        } catch (IOException e) {
            logger.error("Exception when close CubeScanner", e);
        }
    }

}
