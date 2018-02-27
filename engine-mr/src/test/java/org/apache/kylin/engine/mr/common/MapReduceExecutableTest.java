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

package org.apache.kylin.engine.mr.common;

import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.apache.kylin.common.HotLoadKylinPropertiesTestCase;
import org.apache.kylin.common.KylinConfig;
import org.junit.Assert;
import org.junit.Test;

public class MapReduceExecutableTest extends HotLoadKylinPropertiesTestCase {

    @Test
    public void testOverwriteJobConf() throws Exception {
        MapReduceExecutable executable = new MapReduceExecutable();
        KylinConfig config = KylinConfig.getInstanceFromEnv();

        Method method = MapReduceExecutable.class.getDeclaredMethod("overwriteJobConf", Configuration.class,
                KylinConfig.class, new String[] {}.getClass());
        method.setAccessible(true);
        Configuration conf = new Configuration();
        conf.set("mapreduce.job.is-mem-hungry", "true");
        method.invoke(executable, conf, config, new String[] { "-cubename", "ci_inner_join_cube" });
        Assert.assertEquals("mem-test1", conf.get("test1"));
        Assert.assertEquals("mem-test2", conf.get("test2"));

        conf.clear();
        method.invoke(executable, conf, config, new String[] { "-cubename", "ci_inner_join_cube" });
        Assert.assertEquals("test1", conf.get("test1"));
        Assert.assertEquals("test2", conf.get("test2"));
    }
}