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

package org.apache.kylin.common.util;

import java.io.File;
import org.apache.kylin.common.KylinConfig;

/**
 * @author ysong1
 */
public class HBaseMetadataTestCase extends AbstractKylinTestCase {

    static {
        try {
            ClassUtil.addClasspath(new File("../examples/test_case_data/sandbox/").getAbsolutePath());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void createTestMetadata() throws Exception {
        staticCreateTestMetadata();
    }

    @Override
    public void cleanupTestMetadata() {
        staticCleanupTestMetadata();
    }

    public static void staticCreateTestMetadata() throws Exception {
        staticCreateTestMetadata(SANDBOX_TEST_DATA);
    }

    public static void staticCreateTestMetadata(String kylinConfigFolder) {

        KylinConfig.destoryInstance();

        if (System.getProperty(KylinConfig.KYLIN_CONF) == null && System.getenv(KylinConfig.KYLIN_CONF) == null)
            System.setProperty(KylinConfig.KYLIN_CONF, kylinConfigFolder);

    }

}
