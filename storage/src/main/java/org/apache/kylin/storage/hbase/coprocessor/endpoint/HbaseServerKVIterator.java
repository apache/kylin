/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.storage.hbase.coprocessor.endpoint;

import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.kylin.invertedindex.model.IIRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Created by honma on 11/10/14.
 */
public class HbaseServerKVIterator implements Iterable<IIRow>, Closeable {

    private RegionScanner innerScanner;
    private Logger logger = LoggerFactory.getLogger(HbaseServerKVIterator.class);

    public HbaseServerKVIterator(RegionScanner innerScanner) {
        this.innerScanner = innerScanner;
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(this.innerScanner);
    }

    private static class IIRowIterator implements Iterator<IIRow> {

        private final RegionScanner regionScanner;
        private final IIRow row = new IIRow();
        List<Cell> results = Lists.newArrayList();

        private boolean hasMore;

        IIRowIterator(RegionScanner innerScanner) {
            this.regionScanner = innerScanner;
            try {
                hasMore = regionScanner.nextRaw(results);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean hasNext() {
            return !results.isEmpty();
        }

        @Override
        public IIRow next() {
            if (results.size() < 1) {
                throw new NoSuchElementException();
            }
            for (Cell c : results) {
                row.updateWith(c);
            }
            results.clear();
            try {
                if (hasMore) {
                    hasMore = regionScanner.nextRaw(results);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return row;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public Iterator<IIRow> iterator() {
        return new IIRowIterator(innerScanner);
    }

}
