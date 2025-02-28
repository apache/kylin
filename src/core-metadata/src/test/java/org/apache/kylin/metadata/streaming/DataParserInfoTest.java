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

package org.apache.kylin.metadata.streaming;

import org.junit.Assert;
import org.junit.Test;

public class DataParserInfoTest {

    @Test
    public void testCreate() {
        {
            String project = "streaming_test";
            String defaultClassName = "org.apache.kylin.parser.TimedJsonStreamParser";
            String jarName = "default";
            DataParserInfo dataParserInfo = new DataParserInfo(project, defaultClassName, jarName);
            dataParserInfo.getStreamingTables().add("table1");
            Assert.assertEquals(project, dataParserInfo.getProject());
            Assert.assertEquals(defaultClassName, dataParserInfo.getClassName());
            Assert.assertEquals(jarName, dataParserInfo.getJarName());
            Assert.assertNotNull(dataParserInfo.getStreamingTables().get(0));
            Assert.assertEquals("streaming_test.org.apache.kylin.parser.TimedJsonStreamParser",
                    dataParserInfo.resourceName());
            Assert.assertEquals("DATA_PARSER/streaming_test.org.apache.kylin.parser.TimedJsonStreamParser",
                    dataParserInfo.getResourcePath());
        }

        {
            DataParserInfo dataParserInfo = new DataParserInfo();
            Assert.assertNull(dataParserInfo.getProject());
            Assert.assertNull(dataParserInfo.getClassName());
            Assert.assertNull(dataParserInfo.getJarName());
            Assert.assertEquals(0, dataParserInfo.getStreamingTables().size());
        }
    }
}
