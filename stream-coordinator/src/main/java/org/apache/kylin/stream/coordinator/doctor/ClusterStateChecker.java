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

import org.apache.hadoop.hbase.util.Threads;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 *
 * <h2>Something need to check</h2>
 * <dl>
 *     <dt>Zookeeper Avaliable</dt>
 *     <dd>Check Avaliable</dd>
 *
 *     <dt>Receiver Avaliable</dt>
 *     <dd>Check Avaliable</dd>
 *
 *     <dt>Metadata RW failure</dt>
 *     <dd>Coordinator write consistent state into metadata, statistics of r/w failure
 *      is important of cluster health</dd>
 *
 *     <dt>RPC failure</dt>
 *     <dd>Coordinator send request to streaming receiver, statistics
 *      of failure is important of cluster health</dd>
 *
 *     <dt>Segment Build Job & Promotion Failure</dt>
 *
 *
 *     <dt>Cube Assignment Consistency</dt>
 *     <dd>If receiver's behvaior is not aligned with central metdadata, it indicated there must be something wrong.</dd>
 *
 *     <dt>Active segments Count & Immutable segments Count</dt>
 *     <dd>If receivers have too many active segments, it indicated that promotion is blocked,
 *      for each query it received, it has to scan too much segment/fragment file,
 *      so performance will be impacted badly.  </dd>
 *
 *     <dt>Consume lag</dt>
 *     <dd>If receivers cannot catch the rate by producer, much active will be accumulated,
 *      and performance will be impacted badly.  </dd>
 *
 * </dl>
 *
 * <h2>Check and Report</h2>
 * Basic step:
 * <ol>
 *  <li> stop coordinator to avoid underlying concurrency issue </li>
 *  <li> check inconsistent state of all receiver cluster </li>
 *  <li> send summary via mail to kylin admin </li>
 *  <li> if need, call ClusterDoctor to repair inconsistent issue </li>
 * </ol>
 *
 * @see org.apache.kylin.stream.coordinator.coordinate.annotations.NotAtomicAndNotIdempotent
 * @see ClusterDoctor
 */
public class ClusterStateChecker {

    ThreadPoolExecutor tpe = new ThreadPoolExecutor(1, 10, 20, TimeUnit.MINUTES,
            new LinkedBlockingQueue<Runnable>(10 * 100), //
            Threads.newDaemonThreadFactory("Cluster-checker-"));




}
