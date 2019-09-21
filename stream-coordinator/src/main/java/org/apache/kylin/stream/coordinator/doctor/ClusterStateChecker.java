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

package org.apache.kylin.stream.coordinator.doctor;

/**
 * <pre>
 * Basic step of this class:
 *  1. stop coordinator to avoid underlying concurrency issue
 *  2. check inconsistent state of all receiver cluster
 *  3. send summary via mail to kylin admin
 *  4. if need, call ClusterDoctor to repair inconsistent issue
 * </pre>
 * @see org.apache.kylin.stream.coordinator.coordinate.annotations.NotAtomicAndNotIdempotent
 * @see ClusterDoctor
 */
public class ClusterStateChecker {
    // TO BE IMPLEMENTED
}
