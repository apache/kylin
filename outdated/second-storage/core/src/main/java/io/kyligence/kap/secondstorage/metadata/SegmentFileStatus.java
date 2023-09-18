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

package io.kyligence.kap.secondstorage.metadata;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonAutoDetect(
        fieldVisibility = JsonAutoDetect.Visibility.NONE,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE,
        setterVisibility = JsonAutoDetect.Visibility.NONE)
public class SegmentFileStatus implements Serializable {
    @JsonProperty("path")
    private String path;
    /**
     * file length in bytes
     */
    @JsonProperty("len")
    private Long len;

    public static SegmentFileStatusBuilder builder() {
        return new SegmentFileStatusBuilder();
    }

    public String getPath() {
        return path;
    }

    public Long getLen() {
        return len;
    }

    public static class SegmentFileStatusBuilder {
        private String path;
        private Long len;

        public SegmentFileStatusBuilder setPath(String path) {
            this.path = path;
            return this;
        }

        public SegmentFileStatusBuilder setLen(Long len) {
            this.len = len;
            return this;
        }

        public SegmentFileStatus build() {
            SegmentFileStatus status = new SegmentFileStatus();
            status.len = len;
            status.path = path;
            return status;
        }
    }
}
