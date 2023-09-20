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

package org.apache.kylin.common.mail;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.kylin.guava30.shaded.common.base.Joiner;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;

public class MailNotificationCreator {
    private static final String MAIL_TITLE_PREFIX = "Kylin System Notification";

    private MailNotificationCreator() {
        throw new IllegalStateException("Utility class");
    }

    public static String createContent(Template template, Map<String, Object> data)
            throws IOException, TemplateException {
        try (Writer out = new StringWriter()) {
            template.process(data, out);
            return out.toString();
        }
    }

    public static String createTitle(MailNotificationType notificationType) {
        return "[" + Joiner.on("]-[").join(MAIL_TITLE_PREFIX, notificationType.getDisplayName()) + "]";
    }

    public static class MailTemplate {
        private static final String MAIL_TEMPLATES_DIR = "/mail_templates";
        private static final Configuration configuration;

        private MailTemplate() {
            throw new IllegalStateException("Utility class");
        }

        static {
            configuration = new Configuration(Configuration.getVersion());
            configuration.setClassForTemplateLoading(MailTemplate.class, MAIL_TEMPLATES_DIR);
            configuration.setDefaultEncoding(StandardCharsets.UTF_8.toString());
        }

        public static Template getTemplate(String templateName) throws IOException {
            return configuration.getTemplate(templateName + ".ftl");
        }
    }
}
