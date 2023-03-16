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
package org.apache.kylin.tool.obf;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import org.apache.kylin.common.util.JsonUtil;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Maps;

public class MappingRecorder implements Closeable {
    private boolean isClosed = false;
    private File output;
    private Map<String, Map<String, String>> mapping = Maps.newConcurrentMap();

    public MappingRecorder(File output) {
        this.output = output;
    }

    public String addMapping(String catalog, String from, String to) {
        Preconditions.checkState(!isClosed);

        if (!mapping.containsKey(catalog)) {
            mapping.put(catalog, Maps.newConcurrentMap());
        }

        mapping.get(catalog).putIfAbsent(from, to);

        return to;
    }

    public String addMapping(ObfCatalog catalog, String from) {
        return addMapping(catalog.value, from, catalog.obf(from));
    }

    public Map<String, Map<String, String>> getMapping() {
        return mapping;
    }

    @Override
    public void close() throws IOException {
        if (!isClosed) {
            store();
            isClosed = true;
            mapping = null;
        }
    }

    public void store() throws IOException {
        if (output == null) {
            return;
        }

        try (OutputStream os = new FileOutputStream(output)) {
            JsonUtil.writeValue(os, mapping);
            os.flush();
        }
    }
}
