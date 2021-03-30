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

package org.apache.kylin.stream.core.query;

import static org.apache.kylin.shaded.com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.kylin.stream.core.storage.Record;


public class SingleThreadResultCollector extends ResultCollector {

    @Override
    public Iterator<Record> iterator() {
        if (searchResults.isEmpty()) {
            return Collections.emptyIterator();
        }
        Iterator<IStreamingSearchResult> resultIterator = searchResults.iterator();
        return new Iterator<Record>() {
            Iterator<Record> current = Collections.emptyIterator();
            IStreamingSearchResult prevResult = null;

            @Override
            public boolean hasNext() {
                boolean currentHasNext;
                while (!(currentHasNext = checkNotNull(current).hasNext()) && resultIterator.hasNext()) {
                    if (prevResult != null) {
                        prevResult.endRead();
                    }
                    IStreamingSearchResult currResult = resultIterator.next();
                    currResult.startRead();
                    prevResult = currResult;
                    current = currResult.iterator();
                }
                if (!currentHasNext && prevResult != null) {
                    prevResult.endRead();
                }
                return currentHasNext;
            }

            @Override
            public Record next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return current.next();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("not support remove");
            }
        };
    }

}
