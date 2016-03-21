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

package org.apache.kylin.storage.hbase.cube.v2.filter;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.metadata.filter.UDF.MassInValueProvider;
import org.apache.kylin.metadata.filter.function.Functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;

public class MassInValueProviderImpl implements MassInValueProvider {
    public static final Logger logger = LoggerFactory.getLogger(MassInValueProviderImpl.class);

    private Set<ByteArray> ret = Sets.newHashSet();

    public MassInValueProviderImpl(Functions.FilterTableType filterTableType, String filterResourceIdentifier, DimensionEncoding encoding) {

        if (filterTableType == Functions.FilterTableType.HDFS) {

            logger.info("Start to load HDFS filter table from " + filterResourceIdentifier);
            Stopwatch stopwatch = new Stopwatch().start();

            FileSystem fileSystem;
            try {
                fileSystem = FileSystem.get(HBaseConfiguration.create());
                InputStream inputStream = fileSystem.open(new Path(filterResourceIdentifier));
                List<String> lines = IOUtils.readLines(inputStream);

                logger.info("Load HDFS finished after " + stopwatch.elapsedMillis() + " millis");

                for (String line : lines) {
                    ByteArray byteArray = ByteArray.allocate(encoding.getLengthOfEncoding());
                    encoding.encode(line.getBytes(), line.getBytes().length, byteArray.array(), 0);
                    ret.add(byteArray);
                }

                logger.info("Mass In values constructed after " + stopwatch.elapsedMillis() + " millis, containing " + ret.size() + " entries");

            } catch (IOException e) {
                throw new RuntimeException("error when loading the mass in values", e);
            }
        } else {
            throw new RuntimeException("HBASE_TABLE FilterTableType Not supported yet");
        }
    }

    @Override
    public Set<?> getMassInValues() {
        return ret;
    }
}
