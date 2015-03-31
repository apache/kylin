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

package org.apache.kylin.common.mr;


import org.apache.kylin.common.util.HadoopUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by Hongbin Ma(Binmahone) on 1/19/15.
 */
public class KylinMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    protected void bindCurrentConfiguration(Configuration conf) {
        HadoopUtil.setCurrentConfiguration(conf);
    }
}
