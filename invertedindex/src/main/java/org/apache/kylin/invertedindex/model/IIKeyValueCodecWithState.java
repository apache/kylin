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

package org.apache.kylin.invertedindex.model;

import java.util.Iterator;
import java.util.LinkedList;

import org.apache.kylin.common.util.FIFOIterable;
import org.apache.kylin.common.util.FIFOIterator;
import org.apache.kylin.invertedindex.index.Slice;
import org.apache.kylin.invertedindex.index.TableRecordInfoDigest;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 */
public class IIKeyValueCodecWithState extends IIKeyValueCodec {

    public IIKeyValueCodecWithState(TableRecordInfoDigest digest) {
        super(digest);
    }

    /**
     * 
     * @param kvs kvs must be a {@link org.apache.kylin.common.util.FIFOIterable } to avoid {@link java.util.ConcurrentModificationException}.
     * @return
     */
    @Override
    public Iterable<Slice> decodeKeyValue(Iterable<IIRow> kvs) {
        if (!(kvs instanceof FIFOIterable)) {
            throw new IllegalArgumentException("kvs must be a {@link org.apache.kylin.common.util.FIFOIterable } to avoid {@link java.util.ConcurrentModificationException}.");
        }
        return new IIRowDecoderWithState(digest, kvs.iterator());
    }

    //TODO refactor this class, does not have to extend IIKeyValueCodec, composite might be enough, mhb
    protected static class IIRowDecoderWithState extends IIRowDecoder {

        final LinkedList<IIRow> buffer = Lists.newLinkedList();
        private Iterator<Slice> superIterator = null;

        private IIRowDecoderWithState(TableRecordInfoDigest digest, Iterator<IIRow> iiRowIterator) {
            super(digest, iiRowIterator);
            this.feedingIterator = new FIFOIterator<>(buffer);
        }

        private Iterator<Slice> getSuperIterator() {
            if (superIterator == null) {
                superIterator = super.iterator();
            }
            return superIterator;
        }

        @Override
        public Iterator<Slice> iterator() {
            return new Iterator<Slice>() {
                @Override
                public boolean hasNext() {
                    while (buffer.size() < incompleteDigest.getColumnCount() && iiRowIterator.hasNext()) {
                        buffer.add(iiRowIterator.next());
                    }
                    return buffer.size() == incompleteDigest.getColumnCount();
                }

                @Override
                public Slice next() {
                    while (buffer.size() < incompleteDigest.getColumnCount() && iiRowIterator.hasNext()) {
                        buffer.add(iiRowIterator.next());
                    }
                    Preconditions.checkArgument(buffer.size() == incompleteDigest.getColumnCount(), "not enough IIRows!");
                    Slice ret = IIRowDecoderWithState.this.getSuperIterator().next();
                    buffer.clear();
                    return ret;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }
}
