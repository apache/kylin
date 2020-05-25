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

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;

public abstract class TracerWrapper {

    private final Tracer tracer;

    TracerWrapper(Tracer tracer) {
        this.tracer = tracer;
    }

    public Span activeSpan() {
        return tracer.activeSpan();
    }

    public Span startSpan(TracerConstants.OperationEum operation, Span parentSpan) {
        return startSpan(operation.toString(), parentSpan);
    }

    public Span startSpan(String operationName, Span parentSpan) {
        Scope scope;
        if (parentSpan != null) {
            scope = tracer.buildSpan(operationName).ignoreActiveSpan().asChildOf(parentSpan).startActive(false);
        } else {
            scope = tracer.buildSpan(operationName).ignoreActiveSpan().startActive(false);
        }
        return scope.span();
    }

    public abstract Object getTagValue(Span span, String tagName);

    // by Millisecond
    public abstract long getStart(Span span);

    // by Millisecond
    public abstract long getDuration(Span span);
}
