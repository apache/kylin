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

package org.apache.kylin.storage.hbase.cube.v2;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exceptions.ResourceLimitExceededException;
import org.apache.kylin.common.tracer.TracerConstants.TagEnum;
import org.apache.kylin.common.util.MailService;
import org.apache.kylin.common.util.MailTemplateProvider;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.common.util.ToolUtil;
import org.apache.kylin.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentracing.Span;

public class HBaseRPCHealthCheck {
    public static final Logger logger = LoggerFactory.getLogger(HBaseRPCHealthCheck.class);

    public static final String QUERY_ALERT = "QUERY_ALERT";

    public static final int KYLIN_ADMIN = 1;
    public static final int KYLIN_USER = 2;
    public static final int KYLIN_HBASE = 3;

    public static final String NA = "NA";

    public static void alert(QueryContext queryContext, Span epRangeSpan, Span regionRPCSpan, String reason,
            int recipient) {
        alertEmail(queryContext, epRangeSpan, regionRPCSpan, reason, "WARN", recipient);
    }

    public static void alert(QueryContext queryContext, Span epRangeSpan, Span regionRPCSpan, Exception exception,
            int recipient) {
        StringWriter out = new StringWriter();
        exception.printStackTrace(new PrintWriter(out));
        if (exception instanceof ResourceLimitExceededException) {
            alertEmail(queryContext, epRangeSpan, regionRPCSpan, out.toString(), "WARN", recipient);
        } else {
            alertEmail(queryContext, epRangeSpan, regionRPCSpan, out.toString(), "ERROR", recipient);
        }
    }

    private static void alertEmail(QueryContext queryContext, Span epRangeSpan, Span regionRPCSpan, String alertReason,
            String state, int recipient) {
        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        if (!kylinConfig.isHBaseBadRegionDetectEnabled()) {
            return;
        }

        try {
            Set<String> users = Sets.newHashSet();
            final String[] kylinAdminDls = kylinConfig.getAdminDls();
            if (kylinAdminDls != null) {
                users.addAll(Sets.newHashSet(kylinAdminDls));
            }

            if (recipient == KYLIN_USER) {
                users.add(queryContext.getUsername());
            } else if (recipient == KYLIN_HBASE) {
                final String[] hbaseAdminDls = kylinConfig.getHBaseAdminDls();
                if (hbaseAdminDls != null) {
                    users.addAll(Sets.newHashSet(hbaseAdminDls));
                }
            }

            if (users.isEmpty()) {
                logger.warn("no need to send email, user list is empty");
                return;
            }
            final Pair<String, String> email = formatNotifications(queryContext, epRangeSpan, regionRPCSpan,
                    alertReason, state);
            if (email == null) {
                logger.warn("no need to send email, content is null");
                return;
            }
            new MailService(kylinConfig).sendMail(Lists.newArrayList(users), email.getFirst(), email.getSecond());
        } catch (Exception e) {
            logger.error("error send email", e);
        }
    }

    @VisibleForTesting
    static Pair<String, String> formatNotifications(QueryContext queryContext, Span epRangeSpan, Span regionRPCSpan,
            String alertReason, String state) {
        Map<String, Object> root = Maps.newHashMap();
        String cube = queryContext.getSpanTagValue(epRangeSpan, TagEnum.CUBE.toString());
        root.put("env_name", KylinConfig.getInstanceFromEnv().getDeployEnv());
        root.put("submitter", StringUtil.noBlank(queryContext.getUsername(), "UNKNOWN"));
        root.put("query_engine", ToolUtil.getHostName());
        root.put("project_name", queryContext.getProject());
        root.put("sql", queryContext.getSql());
        root.put("queryId", queryContext.getQueryId());
        root.put("cube_name", cube);
        if (queryContext.getSpanTagValue(epRangeSpan, TagEnum.SEGMENT.toString()) != null) {
            root.put("segment_name", queryContext.getSpanTagValue(epRangeSpan, TagEnum.SEGMENT.toString()));
        } else {
            root.put("segment_name", NA);
        }
        if (queryContext.getSpanTagValue(epRangeSpan, TagEnum.HTABLE.toString()) != null) {
            root.put("htable", queryContext.getSpanTagValue(epRangeSpan, TagEnum.HTABLE.toString()));
        } else {
            root.put("htable", NA);
        }
        if (queryContext.getSpanTagValue(regionRPCSpan, TagEnum.REGION_SERVER.toString()) != null) {
            root.put("region_server", queryContext.getSpanTagValue(regionRPCSpan, TagEnum.REGION_SERVER.toString()));
        } else {
            root.put("region_server", NA);
        }
        root.put("rpc_duration", queryContext.getSpanDuration(regionRPCSpan) + "(ms)");
        if (queryContext.getSpanTagValue(regionRPCSpan, TagEnum.RPC_DURATION.toString()) != null) {
            root.put("kylin_ep_duration", queryContext.getSpanTagValue(regionRPCSpan, TagEnum.RPC_DURATION.toString()));
        } else {
            root.put("kylin_ep_duration", NA);
        }

        root.put("alert_reason", Matcher.quoteReplacement(StringUtil.noBlank(alertReason, "no error message")));

        String content = MailTemplateProvider.getInstance().buildMailContent(QUERY_ALERT, root);
        String title = MailTemplateProvider.getMailTitle("QUERY ALERT", state,
                KylinConfig.getInstanceFromEnv().getDeployEnv(), queryContext.getProject(), cube);
        return Pair.newPair(title, content);
    }
}
