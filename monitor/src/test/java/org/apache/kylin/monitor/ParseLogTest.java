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

package org.apache.kylin.monitor;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Created by jiazhong on 2015/5/27.
 */
public class ParseLogTest {

    private QueryParser queryParser;
    private ApiRequestParser apiReqParser;

    @BeforeClass
    public static void beforeClass() throws Exception {
        //set catalina.home temp
        System.setProperty(ConfigUtils.CATALINA_HOME, "../server/");

        //test_case_data/sandbox/ contains HDP 2.2 site xmls which is dev sandbox
        ConfigUtils.addClasspath(new File("../examples/test_case_data/sandbox").getAbsolutePath());

        //get log base dir
        System.setProperty(ConfigUtils.KYLIN_LOG_CONF_HOME, "../examples/test_case_data/performance_data");

        //get kylin.properties
        System.setProperty(ConfigUtils.KYLIN_CONF, "../examples/test_case_data/sandbox");

    }

    @Before
    public void before() {
        queryParser = new QueryParser();
        apiReqParser = new ApiRequestParser();
    }

    @Test
    public void test() throws Exception {
        testQueryParseing();
        testApiReqParsing();
    }

    private void testQueryParseing() throws IOException, ParseException {
        queryParser.getQueryLogFiles();
    }

    private void testApiReqParsing() {
        apiReqParser.getRequestLogFiles();
    }

}