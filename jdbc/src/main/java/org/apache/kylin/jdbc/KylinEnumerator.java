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

package org.apache.kylin.jdbc;

import java.util.Collection;
import java.util.Iterator;

import org.apache.calcite.linq4j.Enumerator;

/**
 * Query results enumerator
 * 
 * @author xduo
 * 
 */
public class KylinEnumerator<E> implements Enumerator<E> {

    /**
     * current row
     */
    private E current;

    /**
     * data collection
     */
    private Collection<E> dataCollection;

    /**
     * result iterator
     */
    private Iterator<E> cursor;

    public KylinEnumerator(Collection<E> dataCollection) {
        this.dataCollection = dataCollection;
        this.cursor = this.dataCollection.iterator();

        if (null == this.cursor) {
            throw new RuntimeException("Cursor can't be null");
        }
    }

    @Override
    public E current() {
        return current;
    }

    @Override
    public boolean moveNext() {
        if (!cursor.hasNext()) {
            this.reset();

            return false;
        }

        current = cursor.next();

        return true;
    }

    @Override
    public void reset() {
        this.cursor = this.dataCollection.iterator();
    }

    @Override
    public void close() {
        this.cursor = null;
        this.dataCollection = null;
    }

}
