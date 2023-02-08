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

package org.apache.kylin.source.hive;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang3.StringUtils;
import org.junit.Ignore;
import org.junit.Test;

public class BeelineOptionsProcessorTest {
    @Ignore
    @Test
    public void foo() {
        String param = "-n root --hiveconf hive.security.authorization.sqlstd.confwhitelist.append='mapreduce.job.*|dfs.*' -u 'jdbc:hive2://localhost:10000'";
        BeelineOptionsProcessor processor = new BeelineOptionsProcessor();
        CommandLine commandLine = processor.process(StringUtils.split(param));
        String n = commandLine.getOptionValue('n');
        String u = commandLine.getOptionValue('u');
        String p = commandLine.getOptionValue('p');

    }
}
