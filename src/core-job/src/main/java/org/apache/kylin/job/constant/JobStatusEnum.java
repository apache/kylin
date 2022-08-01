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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public enum JobStatusEnum {

    NEW(0) {
        @Override
        public boolean checkAction(JobActionEnum actionEnum) {
            return true;
        }
    },
    PENDING(1) {
        @Override
        public boolean checkAction(JobActionEnum actionEnum) {
            return actionEnum == JobActionEnum.PAUSE || actionEnum == JobActionEnum.DISCARD;
        }
    },
    RUNNING(2) {
        @Override
        public boolean checkAction(JobActionEnum actionEnum) {
            return actionEnum == JobActionEnum.PAUSE || actionEnum == JobActionEnum.DISCARD
                    || actionEnum == JobActionEnum.RESTART;
        }
    },
    FINISHED(4) {
        @Override
        public boolean checkAction(JobActionEnum actionEnum) {
            return false;
        }
    },
    ERROR(8) {
        @Override
        public boolean checkAction(JobActionEnum actionEnum) {
            return actionEnum == JobActionEnum.DISCARD || actionEnum == JobActionEnum.RESUME
                    || actionEnum == JobActionEnum.RESTART;
        }
    },
    DISCARDED(16) {
        @Override
        public boolean checkAction(JobActionEnum actionEnum) {
            return false;
        }
    },
    STOPPED(32) {
        @Override
        public boolean checkAction(JobActionEnum actionEnum) {
            return actionEnum == JobActionEnum.DISCARD || actionEnum == JobActionEnum.RESUME
                    || actionEnum == JobActionEnum.RESTART;
        }
    },
    SUICIDAL(64) {
        @Override
        public boolean checkAction(JobActionEnum actionEnum) {
            return false;
        }
    },
    STARTING(128) {
        @Override
        public boolean checkAction(JobActionEnum actionEnum) {
            return false;
        }
    },
    STOPPING(256) {
        @Override
        public boolean checkAction(JobActionEnum actionEnum) {
            return false;
        }
    },
    LAUNCHING_ERROR(512) {
        @Override
        public boolean checkAction(JobActionEnum actionEnum) {
            return actionEnum == JobActionEnum.DISCARD || actionEnum == JobActionEnum.RESUME
                    || actionEnum == JobActionEnum.RESTART;
        }
    },
    SKIP(1024) {
        @Override
        public boolean checkAction(JobActionEnum actionEnum) {
            return false;
        }
    };

    public abstract boolean checkAction(JobActionEnum actionEnum);

    public String getValidActions() {
        return Arrays.stream(JobActionEnum.values()).filter(this::checkAction).map(JobActionEnum::name)
                .collect(Collectors.joining(", "));
    }

    private final int code;

    private JobStatusEnum(int statusCode) {
        this.code = statusCode;
    }

    public int getCode() {
        return this.code;
    }

    private static final Map<Integer, JobStatusEnum> codeMap = new HashMap<>(10);
    private static final Map<String, JobStatusEnum> nameMap = new HashMap<>(10);
    static {
        for (JobStatusEnum jobStatusEnum : JobStatusEnum.values()) {
            codeMap.put(jobStatusEnum.getCode(), jobStatusEnum);
            nameMap.put(jobStatusEnum.name(), jobStatusEnum);
        }
    }

    public static JobStatusEnum getByCode(Integer statusCode) {
        if (null == statusCode) {
            return null;
        }

        return codeMap.get(statusCode);
    }

    public static JobStatusEnum getByName(String name) {
        return nameMap.get(name);
    }

}
