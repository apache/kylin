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

package org.apache.kylin.storage.hbase.coprocessor.endpoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author honma
 */
public class EndpointEnabler {

    private static final Logger logger = LoggerFactory.getLogger(EndpointEnabler.class);

    static final String FORCE_COPROCESSOR = "forceEndpoint";

    public static boolean isCoprocessorBeneficial() {
        return Boolean.parseBoolean(getForceCoprocessor());
    }

    public static void forceCoprocessorOn() {
        System.setProperty(FORCE_COPROCESSOR, "true");
    }

    public static void forceCoprocessorOff() {
        System.setProperty(FORCE_COPROCESSOR, "false");
    }

    public static String getForceCoprocessor() {
        return System.getProperty(FORCE_COPROCESSOR);
    }

    public static void forceCoprocessorUnset() {
        System.clearProperty(FORCE_COPROCESSOR);
    }

}
