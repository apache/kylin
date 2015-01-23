/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kylinolap.job.invertedindex;

import com.kylinolap.job.dao.ExecutablePO;
import com.kylinolap.job.execution.DefaultChainedExecutable;

/**
 * Created by shaoshi on 1/15/15.
 */
public class IIJob extends DefaultChainedExecutable {

    public IIJob() {
        super();
    }

    private static final String II_INSTANCE_NAME = "iiName";
    private static final String SEGMENT_ID = "segmentId";


    void setIIName(String name) {
        setParam(II_INSTANCE_NAME, name);
    }

    public String getIIName() {
        return getParam(II_INSTANCE_NAME);
    }

    void setSegmentId(String segmentId) {
        setParam(SEGMENT_ID, segmentId);
    }

    public String getSegmentId() {
        return getParam(SEGMENT_ID);
    }

}
