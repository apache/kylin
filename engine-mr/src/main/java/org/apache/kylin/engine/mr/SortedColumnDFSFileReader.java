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
package org.apache.kylin.engine.mr;

import org.apache.kylin.source.ReadableTable;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * Created by xiefan on 16-11-22.
 */
public class SortedColumnDFSFileReader implements ReadableTable.TableReader {
    private Collection<ReadableTable.TableReader> readers;

    @SuppressWarnings("unused")
    private Comparator<String> comparator;

    private PriorityQueue<ReaderBuffer> pq;

    private String[] row;

    public SortedColumnDFSFileReader(Collection<ReadableTable.TableReader> readers, final Comparator<String> comparator) {
        this.readers = readers;
        this.comparator = comparator;
        pq = new PriorityQueue<ReaderBuffer>(11, new Comparator<ReaderBuffer>() {
            @Override
            public int compare(ReaderBuffer i, ReaderBuffer j) {
                boolean isEmpty1 = i.empty();
                boolean isEmpty2 = j.empty();
                if (isEmpty1 && isEmpty2)
                    return 0;
                if (isEmpty1 && !isEmpty2)
                    return 1;
                if (!isEmpty1 && isEmpty2)
                    return -1;
                return comparator.compare(i.peek()[0], j.peek()[0]);
            }
        });
        for (ReadableTable.TableReader reader : readers) {
            if (reader != null) {
                try {
                    pq.add(new ReaderBuffer(reader));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public boolean next() throws IOException {
        while (pq.size() > 0) {
            ReaderBuffer buffer = pq.poll();
            String[] minEntry = buffer.pop();
            this.row = minEntry;
            if (buffer.empty()) {
                pq.remove(buffer);
            } else {
                pq.add(buffer); // add it back
            }
            if (this.row == null) { //avoid the case of empty file
                return false;
            }
            return true;
        }
        return false;
    }

    @Override
    public String[] getRow() {
        return this.row;
    }

    @Override
    public void close() throws IOException {
        for (ReadableTable.TableReader reader : readers)
            reader.close();
    }

    static class ReaderBuffer {
        private ReadableTable.TableReader reader;

        private String[] row;

        public ReaderBuffer(ReadableTable.TableReader reader) throws IOException {
            this.reader = reader;
            reload();
        }

        public void close() throws IOException {
            if (this.reader != null)
                reader.close();
        }

        public boolean empty() {
            return (this.row == null);
        }

        public String[] peek() {
            return this.row;
        }

        public String[] pop() throws IOException {
            String[] result = this.row;
            reload();
            return result;
        }

        private void reload() throws IOException {
            if (reader.next()) {
                row = reader.getRow();
            } else {
                this.row = null;
            }
        }

    }
}
