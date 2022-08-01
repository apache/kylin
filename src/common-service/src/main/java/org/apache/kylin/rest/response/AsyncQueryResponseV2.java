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

package org.apache.kylin.rest.response;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class AsyncQueryResponseV2 {

    private String queryID;
    private AsyncQueryResponse.Status status;
    private String info;

    public static AsyncQueryResponseV2 from(AsyncQueryResponse response) {
        AsyncQueryResponseV2 responseV2 = new AsyncQueryResponseV2();
        responseV2.queryID = response.getQueryID();
        responseV2.status = response.getStatus();
        responseV2.info = response.getInfo();
        return responseV2;
    }
}
