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

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.File;


/**
 * Created by jiazhong on 2015/5/7.
 */
public class Client {


    static {
        //set monitor log path
        String KYLIN_HOME = ConfigUtils.getKylinHome();
        String CATALINA_HOME = null;
        if(!StringUtils.isEmpty(KYLIN_HOME)){
            CATALINA_HOME = ConfigUtils.getKylinHome() + File.separator + "tomcat"+File.separator;
            System.out.println("will use "+CATALINA_HOME+"/logs to put monitor log");
        }else{
            CATALINA_HOME = "";
            System.out.println("will use default path to put monitor log");
        }
        //log4j config will use this
        System.setProperty("CATALINA_HOME",CATALINA_HOME);
    }
    final static Logger logger = Logger.getLogger(Client.class);



    public static void main(String[] args) {
        logger.info("monitor client start parsing...");
        QueryParser queryParser = new QueryParser();
        HiveJdbcClient jdbcClient = new HiveJdbcClient();
        try {
            queryParser.start();
            jdbcClient.start();
        } catch (Exception e) {
            logger.info("Exception",e);
        }
    }
}
