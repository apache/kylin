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
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import freemarker.template.Configuration;
import freemarker.template.Template;

/**
 * Use a key to find a template for email.
 *
 * The template file is [KEY].ftl file under /mail_templates directory with classloader.
 */
public class MailTemplateProvider {

    private static final Logger logger = LoggerFactory.getLogger(MailTemplateProvider.class);

    private static MailTemplateProvider DEFAULT_INSTANCE = new MailTemplateProvider();

    public static MailTemplateProvider getInstance() {
        return DEFAULT_INSTANCE;
    }

    private final Configuration configuration;

    private MailTemplateProvider() {
        configuration = new Configuration(Configuration.getVersion());
        configuration.setClassForTemplateLoading(MailTemplateProvider.class, "/mail_templates");
        configuration.setDefaultEncoding("UTF-8");
    }

    public String buildMailContent(String tplKey, Map<String, Object> data) {
        try {
            Template template = getTemplate(tplKey);
            if (template == null) {
                return "Cannot find email template for " + tplKey;
            }

            try (Writer out = new StringWriter()) {
                template.process(data, out);
                return out.toString();
            }
        } catch (Throwable e) {
            return e.getLocalizedMessage();
        }
    }

    private Template getTemplate(String tplKey) throws Throwable {
        if (StringUtils.isEmpty(tplKey)) {
            return null;
        }
        return configuration.getTemplate(tplKey + ".ftl");
    }
}
