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
package org.apache.kylin.common.htrace;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.shaded.htrace.org.apache.htrace.HTraceConfiguration;
import org.apache.kylin.shaded.htrace.org.apache.htrace.SpanReceiver;
import org.apache.kylin.shaded.htrace.org.apache.htrace.Trace;
import org.apache.kylin.shaded.htrace.org.apache.htrace.impl.LocalFileSpanReceiver;
import org.apache.kylin.shaded.htrace.org.apache.htrace.impl.StandardOutSpanReceiver;
import org.apache.kylin.shaded.htrace.org.apache.htrace.impl.ZipkinSpanReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class HtraceInit {
    public static final Logger logger = LoggerFactory.getLogger(HtraceInit.class);

    static {
        try {
            // init for HTrace
            String fileName = System.getProperty("spanFile");

            Collection<SpanReceiver> rcvrs = new HashSet<SpanReceiver>();

            // writes spans to a file if one is provided to maven with
            // -DspanFile="FILENAME", otherwise writes to standard out.
            if (fileName != null) {
                File f = new File(fileName);
                File parent = f.getParentFile();
                if (parent != null && !parent.exists() && !parent.mkdirs()) {
                    throw new IllegalArgumentException("Couldn't create file: " + fileName);
                }
                HashMap<String, String> conf = new HashMap<String, String>();
                conf.put("local-file-span-receiver.path", fileName);
                LocalFileSpanReceiver receiver = new LocalFileSpanReceiver(HTraceConfiguration.fromMap(conf));
                rcvrs.add(receiver);
            } else {
                rcvrs.add(new StandardOutSpanReceiver(HTraceConfiguration.EMPTY));
            }

            String hostKey = "zipkin.collector-hostname";
            String host = System.getProperty(hostKey);
            String portKey = "zipkin.collector-port";
            String port = System.getProperty(portKey);

            Map<String, String> confMap = Maps.newHashMap();
            if (!StringUtils.isEmpty(host)) {
                confMap.put(hostKey, host);
                logger.info("{} is set to {}", hostKey, host);
            }
            if (!StringUtils.isEmpty(port)) {
                confMap.put(portKey, port);
                logger.info("{} is set to {}", portKey, port);
            }

            ZipkinSpanReceiver zipkinSpanReceiver = new ZipkinSpanReceiver(HTraceConfiguration.fromMap(confMap));
            rcvrs.add(zipkinSpanReceiver);

            for (SpanReceiver receiver : rcvrs) {
                Trace.addReceiver(receiver);
                logger.info("SpanReceiver {} added.", receiver);
            }
        } catch (Exception e) {
            //
            logger.error("Failed to init HTrace", e);
        }
    }

    public static void init() { //only to trigger static block
    }
}
