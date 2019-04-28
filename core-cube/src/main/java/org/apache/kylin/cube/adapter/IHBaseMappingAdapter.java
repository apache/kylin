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

package org.apache.kylin.cube.adapter;

import org.apache.kylin.cube.model.CubeDesc;

/**
 * Initialize hbaseMappingDesc in CubeDesc to adapt hbase storage
 * @author simple
 */
public interface IHBaseMappingAdapter {

    /**
     * Normalize the column family, column and used measure
     * @param cubeDesc
     */
    public void initHBaseMapping(CubeDesc cubeDesc);

    /**
     * Build measure reference in HBaseColumnDesc and make sure that every measure has been assigned
     * @param cubeDesc
     */
    public void initMeasureReferenceToColumnFamilyWithChecking(CubeDesc cubeDesc);
}
