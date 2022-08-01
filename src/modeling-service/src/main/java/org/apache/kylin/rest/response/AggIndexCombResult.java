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

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;

@Getter
public class AggIndexCombResult<T extends Serializable> implements Serializable {

    @JsonProperty("status")
    private String status;
    @JsonProperty("result")
    private T result;

    private AggIndexCombResult(String status, T result) {
        this.status = status;
        this.result = result;
    }

    public static <E extends Serializable> AggIndexCombResult successResult(E result) {
        return new AggIndexCombResult<>("SUCCESS", result);
    }

    public static AggIndexCombResult errorResult() {
        return new AggIndexCombResult<>("FAIL", "invalid number");
    }

    public static AggIndexCombResult combine(AggIndexCombResult r1, AggIndexCombResult r2) {
        if (r1.isFail() || r2.isFail()) {
            return errorResult();
        } else {
            return successResult((Long) r1.result + (Long) r2.result);
        }
    }

    private boolean isFail() {
        return status.equals("FAIL");
    }
}
