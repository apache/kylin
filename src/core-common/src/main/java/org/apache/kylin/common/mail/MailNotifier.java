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

import java.util.List;
import java.util.Objects;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MailNotifier {

    protected static final Logger logger = LoggerFactory.getLogger(MailNotifier.class);

    private MailNotifier() {
        throw new IllegalStateException("Utility class");
    }

    public static boolean notifyUser(KylinConfig kylinConfig, Pair<String, String> mail, List<String> users) {
        if (CollectionUtils.isEmpty(users)) {
            logger.warn("user list is empty, not need to notify users.");
            return false;
        }

        if (Objects.isNull(mail)) {
            logger.error("mail content is null, not need to notify users.");
            return false;
        }

        logger.info("prepare to send email {} to: {}", mail.getFirst(), users);
        try {
            return new MailService(kylinConfig).sendMail(users, mail.getFirst(), mail.getSecond());
        } catch (Exception e) {
            logger.error("notify user {} failed!", mail.getFirst(), e);
            return false;
        }
    }
}
