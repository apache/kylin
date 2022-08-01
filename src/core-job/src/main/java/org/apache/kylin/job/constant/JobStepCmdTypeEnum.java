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

package org.apache.kylin.job.constant;

/**
 * @author xduo, ysong1
 *
 */
public enum JobStepCmdTypeEnum {
    SHELL_CMD, SHELL_CMD_HADOOP, JAVA_CMD_HADOOP_FACTDISTINCT, JAVA_CMD_HADOOP_BASECUBOID, JAVA_CMD_HADOOP_NDCUBOID, JAVA_CMD_HADOOP_RANGEKEYDISTRIBUTION, JAVA_CMD_HADOOP_CONVERTHFILE, JAVA_CMD_HADOOP_MERGECUBOID, JAVA_CMD_HADOOP_NO_MR_DICTIONARY, JAVA_CMD_HADDOP_NO_MR_CREATEHTABLE, JAVA_CMD_HADOOP_NO_MR_BULKLOAD
}
