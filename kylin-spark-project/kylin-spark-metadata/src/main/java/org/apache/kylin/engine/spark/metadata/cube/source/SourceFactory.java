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

package org.apache.kylin.engine.spark.metadata.cube.source;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ImplementationSwitch;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.source.ISource;

public class SourceFactory {

    // Use thread-local because KylinConfig can be thread-local and implementation might be different among multiple threads.
    private static ThreadLocal<ImplementationSwitch<ISource>> sources = new ThreadLocal<>();

    public static ISource getSource(int sourceType) {
        ImplementationSwitch<ISource> current = sources.get();
        if (current == null) {
            current = new ImplementationSwitch<>(KylinConfig.getInstanceFromEnv().getSourceEngines(), ISource.class);
            sources.set(current);
        }
        return current.get(sourceType);
    }

    public static ISource getDefaultSource() {
        return getSource(KylinConfig.getInstanceFromEnv().getDefaultSource());
    }

    public static ISource getCSVSource() {
        return getSource(ISourceAware.ID_CSV);
    }

    public static ISource getSource(ISourceAware aware) {
        return getSource(aware.getSourceType());
    }

    public static <T> T createEngineAdapter(ISourceAware table, Class<T> engineInterface) {
        return getSource(table).adaptToBuildEngine(engineInterface);
    }
}
