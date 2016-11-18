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

package org.apache.kylin.dict;

import java.io.IOException;
import java.util.Arrays;

import org.apache.kylin.source.ReadableTable;

/**
 * Created by dongli on 10/29/15.
 */
public class TableColumnValueEnumerator implements IDictionaryValueEnumerator {

    private ReadableTable.TableReader reader;
    private int colIndex;
    private String colValue;

    public TableColumnValueEnumerator(ReadableTable.TableReader reader, int colIndex) {
        this.reader = reader;
        this.colIndex = colIndex;
    }

    @Override
    public boolean moveNext() throws IOException {
        if (reader.next()) {
            String colStrValue;
            String[] split = reader.getRow();
            if (split.length == 1) {
                colStrValue = split[0];
            } else {
                // normal case
                if (split.length <= colIndex) {
                    throw new ArrayIndexOutOfBoundsException("Column no. " + colIndex + " not found, line split is " + Arrays.asList(split));
                }
                colStrValue = split[colIndex];
            }

            colValue = colStrValue;
            return true;

        } else {
            colValue = null;
            return false;
        }
    }

    @Override
    public void close() throws IOException {
        if (reader != null)
            reader.close();
    }

    @Override
    public String current() {
        return colValue;
    }
}
