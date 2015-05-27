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

import org.apache.log4j.Logger;
import java.io.File;


/**
 * Created by jiazhong on 2015/5/7
 */
public class DebugClient {

    static{
        //set catalina.home temp
        System.setProperty(ConfigUtils.CATALINA_HOME, "../server/");
    }

    final static Logger logger = Logger.getLogger(DebugClient.class);


    public static void main(String[] args) throws Exception {

        // test_case_data/sandbox/ contains HDP 2.2 site xmls which is dev sandbox
        ConfigUtils.addClasspath(new File("../examples/test_case_data/sandbox").getAbsolutePath());

        //set log base dir ,will also get from $KYLIN_HOME/tomcat/logs and config [ext.log.base.dir] in kylin.properties
        System.setProperty(ConfigUtils.KYLIN_LOG_CONF_HOME, "../server/logs");

        //get kylin.properties ,if not exist will get from $KYLIN_HOME/conf/
        System.setProperty(ConfigUtils.KYLIN_CONF, "../examples/test_case_data/sandbox");


        QueryParser queryParser = new QueryParser();
        HiveJdbcClient jdbcClient = new HiveJdbcClient();

        try {
            queryParser.start();
            jdbcClient.start();
        } catch (Exception e) {
            logger.info("Exception ",e);
        }
    }

}