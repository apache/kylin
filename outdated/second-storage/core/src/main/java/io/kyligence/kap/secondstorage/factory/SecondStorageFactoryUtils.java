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

package io.kyligence.kap.secondstorage.factory;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;

import io.kyligence.kap.secondstorage.config.SecondStorageProperties;
import io.kyligence.kap.secondstorage.database.DatabaseOperator;
import io.kyligence.kap.secondstorage.database.QueryOperator;
import io.kyligence.kap.secondstorage.ddl.DDLOperator;
import io.kyligence.kap.secondstorage.metadata.MetadataOperator;

public class SecondStorageFactoryUtils {
    private static final Map<Class<? extends SecondStorageFactory>, SecondStorageFactory> FACTORY_MAP = new ConcurrentHashMap<>();

    private SecondStorageFactoryUtils() {
        // can't new
    }
    
    public static void register(Class<? extends SecondStorageFactory> type, SecondStorageFactory factory) {
        Preconditions.checkArgument(type.isAssignableFrom(factory.getClass()), String.format(Locale.ROOT, "type %s is not assignable from %s",
                type.getName(), factory.getClass().getName()));
        FACTORY_MAP.put(type, factory);
    }


    public static MetadataOperator createMetadataOperator(SecondStorageProperties properties) {
        SecondStorageMetadataFactory factory = (SecondStorageMetadataFactory) FACTORY_MAP.get(SecondStorageMetadataFactory.class);
        return factory.createMetadataOperator(properties);
    }

    public static DatabaseOperator createDatabaseOperator(String jdbcUrl) {
        SecondStorageDatabaseOperatorFactory factory = (SecondStorageDatabaseOperatorFactory) FACTORY_MAP.get(SecondStorageDatabaseOperatorFactory.class);
        return factory.createDatabaseOperator(jdbcUrl);
    }

    public static QueryOperator createQueryMetricOperator(String project) {
        SecondStorageQueryOperatorFactory factory = (SecondStorageQueryOperatorFactory) FACTORY_MAP.get(SecondStorageQueryOperatorFactory.class);
        return factory.getQueryOperator(project);
    }

    public static DDLOperator createSecondaryDDLOperator(){
        SecondStorageIndexFactory factory = (SecondStorageIndexFactory) FACTORY_MAP.get(SecondStorageIndexFactory.class);
        return factory.createDDLOperator();
    }
}
