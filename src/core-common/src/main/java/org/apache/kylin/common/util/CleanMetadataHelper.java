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
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;

/**
 * some local tests only need :
 * 1. a clean kylin.properties
 * 2. a clean local meta store
 * 3. a clean local working dir (kylin.env.hdfs-working-dir)
 */
public class CleanMetadataHelper {
    File jam = null;

    public void setUpWithSomeProperties(List<String> properties) throws IOException {
        jam = File.createTempFile("CleanMetadataHelper", "jam");
        jam.delete();
        jam.mkdirs();
        new File(jam.getAbsolutePath() + "/metadata").mkdir();
        File tempKylinProperties = new File(jam, "kylin.properties");
        jam.deleteOnExit();
        tempKylinProperties.createNewFile();

        if (properties != null) {
            FileUtils.writeLines(tempKylinProperties, properties);
        }

        //implicitly set KYLIN_CONF
        KylinConfig.setKylinConfigForLocalTest(jam.getCanonicalPath());
    }

    public void setUp() throws IOException {
        setUpWithSomeProperties(null);
    }

    public void tearDown() {
        jam.delete();
        Unsafe.clearProperty(KylinConfig.KYLIN_CONF);
        KylinConfig.destroyInstance();
    }
}
