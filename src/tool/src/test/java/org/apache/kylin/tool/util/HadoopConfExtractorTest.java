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

package org.apache.kylin.tool.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.junit.annotation.OverwriteProp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@MetadataInfo(onlyProps = true)
class HadoopConfExtractorTest {

    @Test
    @OverwriteProp(key = "kylin.job.yarn-app-rest-check-status-url", value = "http://127.0.0.1:8080")
    void testExtractYarnMasterHost() {
        String masterUrl = HadoopConfExtractor.extractYarnMasterUrl(new Configuration());
        Assertions.assertEquals("http://127.0.0.1:8080", masterUrl);
    }

    @Test
    void testDefaultExtractYarnMasterHost() {
        String masterUrl = HadoopConfExtractor.extractYarnMasterUrl(new Configuration());
        Assertions.assertEquals("http://0.0.0.0:8088", masterUrl);
    }
}
