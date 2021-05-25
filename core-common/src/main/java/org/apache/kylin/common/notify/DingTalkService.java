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

package org.apache.kylin.common.notify;

import com.dingtalk.api.DefaultDingTalkClient;
import com.dingtalk.api.DingTalkClient;
import com.dingtalk.api.request.OapiRobotSendRequest;
import com.dingtalk.api.response.OapiRobotSendResponse;
import com.taobao.api.ApiException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.notify.util.DingTalkNotificationUtil;
import org.apache.kylin.common.notify.util.NotificationConstants;
import org.apache.kylin.common.notify.util.SecretKeyUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.shaded.com.google.common.base.Strings;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DingTalkService extends NotifyServiceBase {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(DingTalkService.class);

    private static final Pattern TOKEN_REGEX = Pattern.compile("(^[a-z0-9A-Z]*):?([a-z0-9A-Z]*)@?(.*)?");
    private static final Pattern PHONE_REGEX = Pattern.compile("1[3456789]\\d{9}");

    private Boolean enabled = Boolean.TRUE;

    private String url = "https://oapi.dingtalk.com/robot/send?access_token=%s";
    private String secretParam = "&timestamp=%s&sign=%s";

    public DingTalkService(NotificationContext notificationContext) {
        super(notificationContext);
        this.enabled = getNotificationContext().getConfig().isNotificationEnabled();
    }

    public boolean sendNotification() {
        if (!enabled) {
            logger.info("DingTalk service is disabled; this DingTalk will not be delivered");
            logger.info("To enable notify service, set 'kylin.job.notification-enabled=true' in kylin.properties");
            return false;
        }
        
        List<String> receivers = getNotificationContext().getReceivers().get(NotificationConstants.NOTIFY_DINGTALK_LIST);
        receivers = receivers.stream().filter(string -> !string.isEmpty()).collect(Collectors.toList());

        if (CollectionUtils.isEmpty(receivers)) {
            logger.warn("no need to send dingtalk, receivers is empty");
            return false;
        } else {
            logger.info("prepare to send dingtalk to:{}", receivers);
            Pair<String[], Map<String, Object>> content = getNotificationContext().getContent();
            String title = DingTalkNotificationUtil.getTitle(content.getFirst());
            String contentDingTalk = DingTalkNotificationUtil.getContent(getNotificationContext().getState(), title, content.getSecond());
            return sendContent(receivers, title, contentDingTalk);
        }
    }

    private boolean sendContent(List<String> receivers, String title, String contentDingTalk) {
        boolean res = false;
        for (String dingTalkInfo : receivers) {
            String token = "";
            String secret = "";
            List<String> phoneList = new ArrayList<String>();
            Matcher tokenMatcher = TOKEN_REGEX.matcher(dingTalkInfo);
            Matcher phoneMatcher = PHONE_REGEX.matcher(dingTalkInfo);
            if (tokenMatcher.matches()) {
                token = tokenMatcher.group(1);
                secret = tokenMatcher.group(2);
            }

            while (phoneMatcher.find()) {
                phoneList.add(phoneMatcher.group());
            }

            Pair<Long, String> secretKey = null;
            if (!StringUtil.isEmpty(secret)) {
                secretKey = SecretKeyUtil.createSecretKey(secret);
            }

            String formartUrl = url;
            if (null != secretKey) {
                formartUrl += secretParam;
                formartUrl = Strings.lenientFormat(formartUrl, token, secretKey.getFirst(), secretKey.getSecond());
            } else {
                formartUrl = Strings.lenientFormat(formartUrl, token);
            }

            try {
                DingTalkClient client = new DefaultDingTalkClient(formartUrl);
                OapiRobotSendRequest request = new OapiRobotSendRequest();
                request.setMsgtype("markdown");
                OapiRobotSendRequest.Markdown markdown = new OapiRobotSendRequest.Markdown();
                if (!phoneList.isEmpty()) {
                    OapiRobotSendRequest.At at = new OapiRobotSendRequest.At();
                    at.setAtMobiles(phoneList);
                    request.setAt(at);
                }
                markdown.setTitle(title);
                markdown.setText(contentDingTalk);
                request.setMarkdown(markdown);
                OapiRobotSendResponse response = client.execute(request);
                res = null != response && response.isSuccess();
                logger.info("send dingtalk notify is {}", res);
            } catch (ApiException e) {
                logger.error("dingtalk notify fail in send !", e);
            }
        }
        return res;
    }

}
