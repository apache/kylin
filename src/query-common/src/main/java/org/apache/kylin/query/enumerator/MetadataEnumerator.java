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

package org.apache.kylin.query.enumerator;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.util.QueryInterruptChecker;

/**
 */
public class MetadataEnumerator implements Enumerator<Object[]> {

    private Object[] current;
    private final List<Object[]> result;
    private Iterator<Object[]> iterator;
    private int scanCount = 0;

    public MetadataEnumerator(OLAPContext olapContext) {

        this.result = olapContext.getColValuesRange();
        reset();
    }

    @Override
    public boolean moveNext() {
        if (scanCount++ % 1000 == 1) {
            QueryInterruptChecker.checkThreadInterrupted("Interrupted at the stage of collecting result",
                    "MetadataEnumerator");
        }

        boolean hasNext = iterator.hasNext();
        if (hasNext) {
            current = iterator.next();
        }
        return hasNext;
    }

    @Override
    public Object[] current() {
        // Note that array copy used to avoid messy result.
        return Arrays.copyOf(current, current.length);
    }

    @Override
    public void reset() {
        this.iterator = result.iterator();
    }

    @Override
    public void close() {
        // Nothing to do.
    }
}
