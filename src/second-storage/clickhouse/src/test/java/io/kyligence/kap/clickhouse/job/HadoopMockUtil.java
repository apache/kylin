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

package io.kyligence.kap.clickhouse.job;


import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.kylin.common.util.HadoopUtil;
import org.mockito.Mockito;

public class HadoopMockUtil {

    public static void mockGetConfiguration(Map<String, String> overrideHadoopConfig) {
        Mockito.mockStatic(HadoopUtil.class);
        Mockito.when(HadoopUtil.getCurrentConfiguration()).thenReturn(getCurrentConfiguration(overrideHadoopConfig));
    }

    public static Configuration getCurrentConfiguration(Map<String, String> overrideHadoopConfig) {
        Configuration conf = healSickConfig(new Configuration());
        // do not cache this conf, or will affect following mr jobs
        if (overrideHadoopConfig != null) {
            overrideHadoopConfig.forEach(conf::set);
        }
        return conf;
    }

    public static Configuration healSickConfig(Configuration conf) {
        // https://issues.apache.org/jira/browse/KYLIN-953
        if (StringUtils.isBlank(conf.get("hadoop.tmp.dir"))) {
            conf.set("hadoop.tmp.dir", "/tmp");
        }
        //  https://issues.apache.org/jira/browse/KYLIN-3064
        conf.set("yarn.timeline-service.enabled", "false");

        return conf;
    }
}
