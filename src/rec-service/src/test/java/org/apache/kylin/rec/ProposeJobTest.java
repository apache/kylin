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

package org.apache.kylin.rec;

import static org.apache.kylin.common.KylinConfig.KYLIN_CONF;

import org.junit.Assert;
import org.junit.Test;

public class ProposeJobTest {

    @Test
    public void testSetKylinConf() {
        String oldConf = System.getProperty(KYLIN_CONF);
        String[] args = new String[] { "abc", "--metaOutput=/tmp2", "--meta=/tmp", "--meta=/tmp3" };
        try {
            ProposerJob.setKylinConf(args);
            Assert.assertEquals("/tmp", System.getProperty(KYLIN_CONF));
        } finally {
            if (oldConf != null) {
                System.setProperty(KYLIN_CONF, oldConf);
            } else {
                System.clearProperty(KYLIN_CONF);
            }
        }
    }
}
