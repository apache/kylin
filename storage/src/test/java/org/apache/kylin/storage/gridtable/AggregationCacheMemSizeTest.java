package org.apache.kylin.storage.gridtable;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.kylin.common.hll.HyperLogLogPlusCounter;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.metadata.measure.BigDecimalSumAggregator;
import org.apache.kylin.metadata.measure.DoubleSumAggregator;
import org.apache.kylin.metadata.measure.HLLCAggregator;
import org.apache.kylin.metadata.measure.LongSumAggregator;
import org.apache.kylin.metadata.measure.MeasureAggregator;
import org.apache.kylin.storage.gridtable.memstore.MemoryBudgetController;
import org.junit.Test;

/** Note: Execute each test alone to get accurate size estimate. */
public class AggregationCacheMemSizeTest {

    public static final int NUM_OF_OBJS = 1000000 / 2;

    interface CreateAnObject {
        Object create();
    }

    @Test
    public void testHLLCAggregatorSize() throws InterruptedException {
        int est = estimateObjectSize(new CreateAnObject() {
            @Override
            public Object create() {
                HLLCAggregator aggr = new HLLCAggregator(10);
                aggr.aggregate(new HyperLogLogPlusCounter(10));
                return aggr;
            }
        });
        System.out.println("HLLC: " + est);
    }

    @Test
    public void testBigDecimalAggregatorSize() throws InterruptedException {
        int est = estimateObjectSize(new CreateAnObject() {
            @Override
            public Object create() {
                return newBigDecimalAggr();
            }

        });
        System.out.println("BigDecimal: " + est);
    }

    private BigDecimalSumAggregator newBigDecimalAggr() {
        BigDecimalSumAggregator aggr = new BigDecimalSumAggregator();
        aggr.aggregate(new BigDecimal("12345678901234567890.123456789"));
        return aggr;
    }

    @Test
    public void testLongAggregatorSize() throws InterruptedException {
        int est = estimateObjectSize(new CreateAnObject() {
            @Override
            public Object create() {
                return newLongAggr();
            }
        });
        System.out.println("Long: " + est);
    }

    private LongSumAggregator newLongAggr() {
        LongSumAggregator aggr = new LongSumAggregator();
        aggr.aggregate(new LongWritable(10));
        return aggr;
    }

    @Test
    public void testDoubleAggregatorSize() throws InterruptedException {
        int est = estimateObjectSize(new CreateAnObject() {
            @Override
            public Object create() {
                return newDoubleAggr();
            }
        });
        System.out.println("Double: " + est);
    }

    private DoubleSumAggregator newDoubleAggr() {
        DoubleSumAggregator aggr = new DoubleSumAggregator();
        aggr.aggregate(new DoubleWritable(10));
        return aggr;
    }

    @Test
    public void testByteArraySize() throws InterruptedException {
        int est = estimateObjectSize(new CreateAnObject() {
            @Override
            public Object create() {
                return new byte[10];
            }
        });
        System.out.println("byte[10]: " + est);
    }

    @Test
    public void testAggregatorArraySize() throws InterruptedException {
        int est = estimateObjectSize(new CreateAnObject() {
            @Override
            public Object create() {
                return new MeasureAggregator[7];
            }
        });
        System.out.println("MeasureAggregator[7]: " + est);
    }

    @Test
    public void testTreeMapSize() throws InterruptedException {
        final SortedMap<byte[], Object> map = new TreeMap<byte[], Object>(new Comparator<byte[]>() {
            @Override
            public int compare(byte[] o1, byte[] o2) {
                return Bytes.compareTo(o1, o2);
            }
        });
        final Random rand = new Random();
        int est = estimateObjectSize(new CreateAnObject() {
            @Override
            public Object create() {
                byte[] key = new byte[10];
                rand.nextBytes(key);
                map.put(key, null);
                return null;
            }
        });
        System.out.println("TreeMap entry: " + (est - 20)); // -20 is to exclude byte[10]
    }

    @Test
    public void testAggregationCacheSize() throws InterruptedException {
        final SortedMap<byte[], Object> map = new TreeMap<byte[], Object>(new Comparator<byte[]>() {
            @Override
            public int compare(byte[] o1, byte[] o2) {
                return Bytes.compareTo(o1, o2);
            }
        });
        final Random rand = new Random();

        long bytesBefore = memLeft();
        byte[] key = null;
        MeasureAggregator<?>[] aggrs = null;
        for (int i = 0; i < NUM_OF_OBJS; i++) {
            key = new byte[10];
            rand.nextBytes(key);
            aggrs = new MeasureAggregator[4];
            aggrs[0] = newBigDecimalAggr();
            aggrs[1] = newLongAggr();
            aggrs[2] = newDoubleAggr();
            aggrs[3] = newDoubleAggr();
            map.put(key, aggrs);
        }

        long bytesAfter = memLeft();
        
        long mapActualSize = bytesBefore - bytesAfter;
        long mapExpectSize = GTAggregateScanner.estimateSizeOfAggrCache(key, aggrs, map.size());
        System.out.println("Actual cache size: " + mapActualSize);
        System.out.println("Expect cache size: " + mapExpectSize);
    }

    private int estimateObjectSize(CreateAnObject factory) throws InterruptedException {
        Object[] hold = new Object[NUM_OF_OBJS];
        long bytesBefore = memLeft();

        for (int i = 0; i < hold.length; i++) {
            hold[i] = factory.create();
        }

        long bytesAfter = memLeft();
        return (int) ((bytesBefore - bytesAfter) / hold.length);
    }

    private long memLeft() throws InterruptedException {
        Runtime.getRuntime().gc();
        Thread.sleep(500);
        return MemoryBudgetController.getSystemAvailBytes();
    }

}
