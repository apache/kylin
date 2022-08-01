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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBUtils {

    private static final Logger logger = LoggerFactory.getLogger(DBUtils.class);

    /**
     * Closes an <code>ResultSet</code> unconditionally.
     * <p>
     * Equivalent to {@link ResultSet#close()}, except any exceptions will be ignored.
     * This is typically used in finally blocks.
     * <p>
     *
     * @param output the ResultSet to close, may be null or already closed
     */
    public static void closeQuietly(final ResultSet rs) {
        closeQuietly((AutoCloseable) rs);
    }

    /**
     * Closes an <code>Statement</code> unconditionally.
     * <p>
     * Equivalent to {@link Statement#close()}, except any exceptions will be ignored.
     * This is typically used in finally blocks.
     * <p>
     *
     * @param output the ResultSet to close, may be null or already closed
     */
    public static void closeQuietly(final Statement stmt) {
        closeQuietly((AutoCloseable) stmt);
    }

    /**
     * Closes an <code>Connection</code> unconditionally.
     * <p>
     * Equivalent to {@link Connection#close()}, except any exceptions will be ignored.
     * This is typically used in finally blocks.
     * <p>
     *
     * @param output the ResultSet to close, may be null or already closed
     */
    public static void closeQuietly(final Connection conn) {
        closeQuietly((AutoCloseable) conn);
    }

    /**
     * Closes a <code>AutoCloseable</code> unconditionally.
     * <p>
     * Equivalent to {@link AutoCloseable#close()}, except any exceptions will be ignored. This is typically used in
     * finally blocks.
     * <p>
     *
     * @param closeable the objects to close, may be null or already closed
     */
    public static void closeQuietly(final AutoCloseable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (final Exception ioe) {
            logger.debug("", ioe);
        }
    }
}
