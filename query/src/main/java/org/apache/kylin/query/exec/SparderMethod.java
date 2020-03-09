/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.apache.kylin.query.exec;

import java.lang.reflect.Method;
import java.util.HashMap;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.tree.Types;

/**
 * Built-in methods in the Spark adapter.
 *
 * @see org.apache.calcite.util.BuiltInMethod
 */
public enum SparderMethod {
    COLLECT(SparkExec.class, "collectToEnumerable", DataContext.class), //
    COLLECT_SCALAR(SparkExec.class, "collectToScalarEnumerable", DataContext.class),
    ASYNC_RESULT(SparkExec.class, "asyncResult", DataContext.class);

    private static final HashMap<Method, SparderMethod> MAP = new HashMap<Method, SparderMethod>();

    static {
        for (SparderMethod method : SparderMethod.values()) {
            MAP.put(method.method, method);
        }
    }

    public final Method method;

    SparderMethod(Class clazz, String methodName, Class... argumentTypes) {
        this.method = Types.lookupMethod(clazz, methodName, argumentTypes);
    }

    public static SparderMethod lookup(Method method) {
        return MAP.get(method);
    }
}

// End SparkMethod.java
