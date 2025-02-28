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

package org.apache.kylin.rest.util;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.PARAMETER_INVALID_SUPPORT_LIST;

import java.io.IOException;
import java.util.Locale;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.request.SegmentsRequest;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

public class SegmentsTypeEnumJsonDeserializer extends JsonDeserializer<SegmentsRequest.SegmentsRequestType> {

    @Override
    public SegmentsRequest.SegmentsRequestType deserialize(JsonParser p, DeserializationContext ctxt)
            throws IOException {
        String segmentsRequestType = String.valueOf(p.getText());
        if (!SegmentsRequest.SegmentsRequestType.REFRESH.name().equals(segmentsRequestType)
                && !SegmentsRequest.SegmentsRequestType.MERGE.name().equals(segmentsRequestType)) {
            throw new KylinException(PARAMETER_INVALID_SUPPORT_LIST, "type",
                    SegmentsRequest.SegmentsRequestType.REFRESH.name() + ","
                            + SegmentsRequest.SegmentsRequestType.MERGE.name());
        }
        return SegmentsRequest.SegmentsRequestType.valueOf(segmentsRequestType.toUpperCase(Locale.ROOT));
    }
}
