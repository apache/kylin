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

package org.apache.kylin.metadata.tuple;

import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

/**
 */
public class CompoundTupleIterator implements ITupleIterator {
    private static final Logger logger = LoggerFactory.getLogger(CompoundTupleIterator.class);
    private List<ITupleIterator> backends;
    private Iterator<ITuple> compoundIterator;

    public CompoundTupleIterator(List<ITupleIterator> backends) {
        Preconditions.checkArgument(backends != null && backends.size() != 0, "backends not exists");
        this.backends = backends;
        this.compoundIterator = Iterators.concat(backends.iterator());
    }

    @Override
    public void close() {
        for (ITupleIterator i : backends) {
            i.close();
        }
    }

    @Override
    public boolean hasNext() {
        return this.compoundIterator.hasNext();
    }

    @Override
    public ITuple next() {
        return this.compoundIterator.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
