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
package org.apache.kylin.engine.spark.mockup;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.IReadableTable;

public class CsvTableReader implements IReadableTable.TableReader {

    private List<String> allLines;
    private int index = -1;

    public CsvTableReader(String baseDir, TableDesc table) {
        String path = new File(baseDir, "data/" + table.getIdentity() + ".csv").getAbsolutePath();
        try {
            allLines = FileUtils.readLines(new File(path));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean next() {
        index++;
        return index < allLines.size() && !StringUtils.isEmpty(allLines.get(index));
    }

    @Override
    public String[] getRow() {
        return allLines.get(index).split(",");
    }

    @Override
    public void close() {
        // do nothing
    }
}
