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

package org.apache.kylin.engine.spark;

import java.util.Map;

import org.apache.kylin.common.util.ImplementationSwitch;
import org.apache.kylin.job.engine.NCubingEngine;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class NSparkCubingEngine implements NCubingEngine {

    private static ThreadLocal<ImplementationSwitch<NSparkCubingStorage>> storages = new ThreadLocal<>();

    @Override
    public Class<?> getSourceInterface() {
        return NSparkCubingSource.class;
    }

    @Override
    public Class<?> getStorageInterface() {
        return NSparkCubingStorage.class;
    }

    public interface NSparkCubingSource {
        /**
         * Get Dataset<Row>
         *
         * @param table, source table
         * @param ss
         * @return the Dataset<Row>, its schema consists of table column's name, for example, [column1,column2,column3]
         */
        Dataset<Row> getSourceData(TableDesc table, SparkSession ss, Map<String, String> parameters);
    }

    public interface NSparkCubingStorage {

        void saveTo(String path, Dataset<Row> data, SparkSession ss);

        Dataset<Row> getFrom(String path, SparkSession ss);
    }
}
