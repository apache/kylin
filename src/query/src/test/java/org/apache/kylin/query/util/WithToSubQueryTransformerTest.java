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
package org.apache.kylin.query.util;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.project.NProjectManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class WithToSubQueryTransformerTest extends NLocalFileMetadataTestCase {

    private static final String project_0 = "default";

    private static final String project_1 = "ssb";

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testEnableReplaceDynamicParams() {
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        projectManager.updateProject(project_0, copyForWrite -> {
            copyForWrite.getOverrideKylinProps().put("kylin.query.replace-dynamic-params-enabled", "true");
        });

        WithToSubQueryTransformer withToSubQueryTransformer = new WithToSubQueryTransformer();
        String originSql = "with t1 as (select * from KYLIN_SALES WHERE SELLER_ID=10000458)\n" + "select * from t1\n"
                + "union select * from KYLIN_SALES WHERE SELLER_ID=10000896\n" + "LIMIT 500";
        {
            String expectedSql = "SELECT *\n" + "FROM (SELECT *\n" + "FROM KYLIN_SALES\n"
                    + "WHERE SELLER_ID = 10000458) AS T1\n" + "UNION\n" + "SELECT *\n" + "FROM KYLIN_SALES\n"
                    + "WHERE SELLER_ID = 10000896\n" + "FETCH NEXT 500 ROWS ONLY";
            String transformedSql = withToSubQueryTransformer.transform(originSql, project_0, null);
            Assert.assertEquals(expectedSql, transformedSql);
        }
        {
            String transformedSql = withToSubQueryTransformer.transform(originSql, project_1, null);
            Assert.assertEquals(originSql, transformedSql);
        }

        projectManager.updateProject(project_0, copyForWrite -> {
            copyForWrite.getOverrideKylinProps().remove("kylin.query.replace-dynamic-params-enabled");
        });
    }

}
