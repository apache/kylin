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

package org.apache.kylin.metadata.cube.planner.algorithm;

import java.math.BigInteger;
import java.util.List;

/**
 * Algorithm to calculate the layout benefit and recommend cost-effective layout list based on the cube statistics.
 */
public interface LayoutRecommendAlgorithm {

    /**
     * Return a list of recommended layouts for the building segment based on expansionRate
     *
     */
    List<BigInteger> recommend(double expansionRate);

    /**
     * Start the Algorithm running
     *
     * @param maxSpaceLimit
     * @return
     */
    List<BigInteger> start(double maxSpaceLimit);

    /**
     * Cancel the Algorithm running
     *
     *  Users can call this method from another thread to can the Algorithm and return the result earlier
     *
     */
    void cancel();
}
