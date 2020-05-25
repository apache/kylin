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

package org.apache.kylin.common.tracer;

import io.jaegertracing.internal.JaegerSpan;
import io.jaegertracing.internal.JaegerTracer;
import io.opentracing.Span;

public class JaegerTracerWrapper extends TracerWrapper {

    JaegerTracerWrapper(JaegerTracer tracer) {
        super(tracer);
    }

    @Override
    public Object getTagValue(Span span, String tagName) {
        return ((JaegerSpan) span).getTags().get(tagName);
    }

    // by Millisecond
    @Override
    public long getStart(Span span) {
        return ((JaegerSpan) span).getStart() / 1000;
    }

    // by Millisecond
    @Override
    public long getDuration(Span span) {
        return ((JaegerSpan) span).getDuration() / 1000;
    }

}
