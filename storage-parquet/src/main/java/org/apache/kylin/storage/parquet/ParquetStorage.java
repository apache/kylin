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

package org.apache.kylin.storage.parquet;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.engine.mr.IMROutput2;
import org.apache.kylin.engine.spark.ISparkOutput;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.storage.IStorage;
import org.apache.kylin.storage.IStorageQuery;
import org.apache.kylin.storage.parquet.cube.CubeStorageQuery;
import org.apache.kylin.storage.parquet.steps.ParquetMROutput;
import org.apache.kylin.storage.parquet.steps.ParquetSparkOutput;

public class ParquetStorage implements IStorage {
    @Override
    public IStorageQuery createQuery(IRealization realization) {
        if (realization.getType() == RealizationType.CUBE) {
            return new CubeStorageQuery((CubeInstance) realization);
        } else {
            throw new IllegalStateException(
                    "Unsupported realization type for ParquetStorage: " + realization.getType());
        }
    }

    @Override
    public <I> I adaptToBuildEngine(Class<I> engineInterface) {
        if (engineInterface == ISparkOutput.class) {
            return (I) new ParquetSparkOutput();
        } else if (engineInterface == IMROutput2.class) {
            return (I) new ParquetMROutput();
        } else{
            throw new RuntimeException("Cannot adapt to " + engineInterface);
        }
    }
}
