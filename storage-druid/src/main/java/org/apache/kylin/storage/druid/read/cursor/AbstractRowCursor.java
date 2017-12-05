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

package org.apache.kylin.storage.druid.read.cursor;

import java.io.IOException;
import java.util.NoSuchElementException;

import org.apache.kylin.measure.bitmap.RoaringBitmapCounterFactory;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.druid.DruidSchema;

import io.druid.collections.bitmap.WrappedImmutableRoaringBitmap;
import io.druid.query.Query;

public abstract class AbstractRowCursor<T> implements RowCursor {
    protected final DruidSchema schema;
    protected final DruidClient<T> client;
    protected final Query<T> query;
    protected final StorageContext context;

    private final Object[] row;
    private boolean fetched; // indicate whether the next row is fetched

    private Cursor<T> resultCursor;
    private T result;
    private int nextRow;

    public AbstractRowCursor(DruidSchema schema, DruidClient<T> client, Query<T> query, StorageContext context) {
        this.schema = schema;
        this.client = client;
        this.query = query;
        this.context = context;
        this.row = new Object[schema.getTotalFieldCount()];
    }

    /**
     * Convert the i-th value in `result` to `rows`.
     * @return true if i-th row exists, false otherwise
     */
    protected abstract boolean convert(T result, int index, Object[] row);

    protected Object convertComplexValue(Object value) {
        if (value instanceof WrappedImmutableRoaringBitmap) {
            WrappedImmutableRoaringBitmap bitmap = (WrappedImmutableRoaringBitmap) value;
            return RoaringBitmapCounterFactory.INSTANCE.newBitmap(bitmap.getBitmap());
        }
        return value;
    }

    @Override
    public boolean hasNext() {
        if (fetched) {
            return true;
        }

        if (resultCursor == null) {
            resultCursor = client.execute(query, context);
        }

        while (result != null || resultCursor.hasNext()) {
            if (result == null) {
                result = resultCursor.next();
            }
            fetched = convert(result, nextRow++, row);
            if (fetched) {
                return true;
            }
            // try next result
            result = null;
            nextRow = 0;
        }

        return false;
    }

    @Override
    public Object[] next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        fetched = false;
        return row;
    }

    @Override
    public void close() throws IOException {
        if (resultCursor != null) {
            resultCursor.close();
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove");
    }
}
