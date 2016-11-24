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
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.source.ReadableTable;

public class MockupReadableTable implements ReadableTable {

    public static ReadableTable newSingleColumnTable(String path, String... values) {
        TableSignature sig = new TableSignature(path, values.length, 0);
        List<String[]> content = new ArrayList<>();
        for (String v : values) {
            content.add(new String[] { v });
        }
        return new MockupReadableTable(content, sig, true);
    }
    
    public static ReadableTable newNonExistTable(String path) {
        TableSignature sig = new TableSignature(path, -1, 0);
        return new MockupReadableTable(null, sig, false);
    }

    private List<String[]> content;
    private TableSignature sig;
    private boolean exists;

    public MockupReadableTable(List<String[]> content, TableSignature sig, boolean exists) {
        this.content = content;
        this.sig = sig;
        this.exists = exists;
    }

    @Override
    public TableReader getReader() throws IOException {
        return new TableReader() {
            int i = -1;

            @Override
            public boolean next() throws IOException {
                if (content == null)
                    return false;

                i++;
                return i < content.size();
            }

            @Override
            public String[] getRow() {
                return content.get(i);
            }

            @Override
            public void close() throws IOException {
            }
        };
    }

    @Override
    public TableSignature getSignature() throws IOException {
        return sig;
    }

    @Override
    public boolean exists() throws IOException {
        return exists;
    }

}
