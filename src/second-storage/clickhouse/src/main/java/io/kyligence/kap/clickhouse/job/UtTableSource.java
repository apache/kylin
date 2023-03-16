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

package io.kyligence.kap.clickhouse.job;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;

import java.io.File;
import java.net.URI;
import java.util.Locale;

public class UtTableSource implements AbstractTableSource {
    private static final String UT_TABLE_FORMAT = "URL('%s/%s' , Parquet)";
    private static final String LOCAL_SCHEMA = "file:";

    @Override
    public String transformFileUrl(String file, String sitePath, URI rootPath) {
        Preconditions.checkArgument(file.startsWith(LOCAL_SCHEMA));
        URI thisPathURI = new File(file.substring(LOCAL_SCHEMA.length())).toURI();
        return String.format(Locale.ROOT, UT_TABLE_FORMAT, sitePath, rootPath.relativize(thisPathURI).getPath());
    }
}
