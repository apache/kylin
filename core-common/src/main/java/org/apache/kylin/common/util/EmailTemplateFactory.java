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

package org.apache.kylin.common.util;

import java.io.StringWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import freemarker.template.Configuration;
import freemarker.template.Template;

public class EmailTemplateFactory {

    private static final Logger logger = LoggerFactory.getLogger(EmailTemplateFactory.class);

    public static final String NA = "NA";

    private static String localHostName;
    static {
        try {
            localHostName = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            localHostName = "UNKNOWN";
        }
    }

    public static String getLocalHostName() {
        return localHostName;
    }

    public static String getEmailTitle(String... titleParts) {
        StringBuilder sb = new StringBuilder();
        for (String part : titleParts) {
            if (sb.length() > 0) {
                sb.append("-");
            }
            sb.append("[" + part + "]");
        }
        return sb.toString();
    }

    private static EmailTemplateFactory instance = new EmailTemplateFactory();

    public static EmailTemplateFactory getInstance() {
        return instance;
    }

    private final Configuration configuration;

    private EmailTemplateFactory() {
        configuration = new Configuration(Configuration.getVersion());
        configuration.setClassForTemplateLoading(EmailTemplateFactory.class, "/templates");
        configuration.setDefaultEncoding("UTF-8");
    }

    public String buildEmailContent(EmailTemplateEnum state, Map<String, Object> root) {
        try {
            Template template = getTemplate(state);
            if (template == null) {
                return "Cannot find email template for " + state;
            }
            try (Writer out = new StringWriter()) {
                template.process(root, out);
                return out.toString();
            }
        } catch (Throwable e) {
            return e.getLocalizedMessage();
        }
    }

    private Template getTemplate(EmailTemplateEnum state) throws Throwable {
        if (state == null) {
            return null;
        }
        return configuration.getTemplate(state.toString() + ".ftl");
    }
}
