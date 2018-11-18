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

package org.apache.kylin.storage.druid.write;

import java.io.IOException;
import java.util.Locale;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.storage.druid.common.MySQLConnector;
import org.apache.kylin.storage.druid.common.SegmentPublishResult;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.util.StringMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import io.druid.jackson.DefaultObjectMapper;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;

public class AnnounceDruidSegment {
    protected static final Logger logger = LoggerFactory.getLogger(AnnounceDruidSegment.class);

    private static final String QUOTE_STRING = "`";
    private static final int DEFAULT_MAX_TRIES = 10;
    private static final String segment_table = KylinConfig.getInstanceFromEnv().getDruidMysqlSegTabel();

    private static final ObjectMapper jsonMapper = new DefaultObjectMapper();

    private final MySQLConnector connector;

    public AnnounceDruidSegment() {
        connector = new MySQLConnector();
    }

    public SegmentPublishResult announceHistoricalSegments(final Set<DataSegment> segments) throws IOException {
        return connector.retryTransaction(new TransactionCallback<SegmentPublishResult>() {
            @Override
            public SegmentPublishResult inTransaction(final Handle handle, final TransactionStatus transactionStatus)
                    throws Exception {
                final Set<DataSegment> inserted = Sets.newHashSet();

                for (final DataSegment segment : segments) {
                    if (announceHistoricalSegment(handle, segment)) {
                        inserted.add(segment);
                    }
                }

                return new SegmentPublishResult(ImmutableSet.copyOf(inserted), true);
            }
        }, 3, DEFAULT_MAX_TRIES);
    }

    /**
     * Attempts to insert a single segment to the database. If the segment already exists, will do nothing; although,
     * this checking is imperfect and callers must be prepared to retry their entire transaction on exceptions.
     *
     * @return true if the segment was added, false if it already existed
     */
    private boolean announceHistoricalSegment(final Handle handle, final DataSegment segment) throws IOException {
        try {
            if (segmentExists(handle, segment)) {
                logger.info("Found {} in DB, not updating DB", segment.getIdentifier());
                return false;
            }

            // SELECT -> INSERT can fail due to races; callers must be prepared to retry.
            // Avoiding ON DUPLICATE KEY since it's not portable.
            // Avoiding try/catch since it may cause inadvertent transaction-splitting.
            handle.createStatement(String.format(Locale.ROOT,
                    "INSERT INTO %1$s (id, dataSource, created_date, start, %2$send%2$s, partitioned, version, used, payload) "
                            + "VALUES (:id, :dataSource, :created_date, :start, :end, :partitioned, :version, :used, :payload)",
                    segment_table, QUOTE_STRING)).bind("id", segment.getIdentifier())
                    .bind("dataSource", segment.getDataSource()).bind("created_date", new DateTime().toString())
                    .bind("start", segment.getInterval().getStart().toString())
                    .bind("end", segment.getInterval().getEnd().toString())
                    .bind("partitioned", !(segment.getShardSpec() instanceof NoneShardSpec))
                    .bind("version", segment.getVersion()).bind("used", true)
                    .bind("payload", jsonMapper.writeValueAsBytes(segment)).execute();
            logger.info("Published segment {} to DB", segment.getIdentifier());
        } catch (Exception e) {
            logger.error("Exception inserting segment {} into DB", segment.getIdentifier(), e);
            throw e;
        }

        return true;
    }

    private boolean segmentExists(final Handle handle, final DataSegment segment) {
        return !handle.createQuery(String.format(Locale.ROOT, "SELECT id FROM %s WHERE id = :identifier", segment_table))
                .bind("identifier", segment.getIdentifier()).map(StringMapper.FIRST).list().isEmpty();
    }
}
