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

package org.apache.kylin.common.persistence;

import java.io.File;
import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Created by honma on 9/18/14.
 */
@Ignore("convenient trial tool for dev")
public class ResourceToolTest {
    @Before
    public void setup() throws Exception {
        ClassUtil.addClasspath(new File("../examples/test_case_data/hadoop-site").getAbsolutePath());
    }

    @Test
    public void test() throws IOException {
        ResourceTool.copy(KylinConfig.createInstanceFromUri("../examples/test_case_data"), KylinConfig.createInstanceFromUri("../examples/test_case_data/kylin.properties"));
    }

}
