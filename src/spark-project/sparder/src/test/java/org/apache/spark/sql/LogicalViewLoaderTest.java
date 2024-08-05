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

package org.apache.spark.sql;

import static org.apache.kylin.common.exception.ServerErrorCode.DDL_CHECK_ERROR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.source.ISource;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourceFactory;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class LogicalViewLoaderTest {

    @Test
    void testCheckConfigIfNeed() {
        checkConfigIfNeed(true);
        checkConfigIfNeed(false);
        checkConfigIfNeed(true);
        checkConfigIfNeed(false);
    }

    void checkConfigIfNeed(boolean throwException) {
        try (MockedStatic<KylinConfig> configMockedStatic = Mockito.mockStatic(KylinConfig.class);
                MockedStatic<SourceFactory> sourceFactoryMockedStatic = Mockito.mockStatic(SourceFactory.class)) {
            KylinConfig config = Mockito.mock(KylinConfig.class);
            ISource source = Mockito.mock(ISource.class);
            ISourceMetadataExplorer sourceMetadataExplorer = Mockito.mock(ISourceMetadataExplorer.class);
            configMockedStatic.when(KylinConfig::getInstanceFromEnv).thenReturn(config);
            sourceFactoryMockedStatic.when(SourceFactory::getSparkSource).thenReturn(source);
            Mockito.when(config.isDDLLogicalViewEnabled()).thenReturn(true);

            if (throwException) {
                Mockito.when(source.getSourceMetadataExplorer()).thenThrow(new RuntimeException("test"));

                try {
                    LogicalViewLoader.checkConfigIfNeed();
                    fail();
                } catch (Exception e) {
                    assertInstanceOf(KylinException.class, e);
                    assertEquals(((KylinException) e).getErrorCodeString(),
                            DDL_CHECK_ERROR.toErrorCode().getCodeString());
                }
            } else {
                Mockito.when(source.getSourceMetadataExplorer()).thenReturn(sourceMetadataExplorer);

                LogicalViewLoader.checkConfigIfNeed();
            }
        }
    }
}
