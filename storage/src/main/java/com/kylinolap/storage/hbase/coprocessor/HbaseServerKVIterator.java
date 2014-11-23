package com.kylinolap.storage.hbase.coprocessor;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Pair;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by honma on 11/10/14.
 */
public class HbaseServerKVIterator implements Iterable<Pair<ImmutableBytesWritable, ImmutableBytesWritable>>, Closeable {

    private RegionScanner innerScaner;

    List<Cell> results = new ArrayList<Cell>();

    public HbaseServerKVIterator(RegionScanner innerScaner) {
        this.innerScaner = innerScaner;
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(this.innerScaner);
    }

    @Override
    public Iterator<Pair<ImmutableBytesWritable, ImmutableBytesWritable>> iterator() {
        return new Iterator<Pair<ImmutableBytesWritable, ImmutableBytesWritable>>() {

            ImmutableBytesWritable key = new ImmutableBytesWritable();
            ImmutableBytesWritable value = new ImmutableBytesWritable();
            Pair<ImmutableBytesWritable, ImmutableBytesWritable> pair = new Pair<>(key, value);

            private boolean hasMore = true;

            @Override
            public boolean hasNext() {
                return hasMore;
            }


            @Override
            public Pair<ImmutableBytesWritable, ImmutableBytesWritable> next() {
                if (!hasNext()) {
                    try {
                        hasMore = innerScaner.nextRaw(results);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    if (results.size() < 1)
                        throw new IllegalStateException("Hbase row contains less than 1 row");

                    Cell c = results.get(0);
                    key.set(c.getRowArray(), c.getRowOffset(), c.getRowLength());
                    value.set(c.getValueArray(), c.getValueOffset(), c.getValueLength());
                    return pair;
                } else {
                    return null;
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

}

