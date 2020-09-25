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

package org.apache.kylin.rest.request;

import org.apache.kylin.shaded.com.google.common.base.Function;
import org.apache.kylin.shaded.com.google.common.collect.FluentIterable;
import org.apache.kylin.shaded.com.google.common.collect.Sets;
import javax.annotation.Nullable;

import java.util.Set;

public class JobOptimizeRequest {

    private Set<String> cuboidsRecommend;

    public Set<Long> getCuboidsRecommend() {
        return Sets.newHashSet(FluentIterable.from(cuboidsRecommend).transform(new Function<String, Long>() {
            @Nullable
            @Override
            public Long apply(@Nullable String cuboid) {
                return Long.valueOf(cuboid);
            }
        }));
    }

    public void setCuboidsRecommend(Set<Long> cuboidsRecommend) {
        this.cuboidsRecommend = Sets.newHashSet(FluentIterable.from(cuboidsRecommend).transform(new Function<Long, String>() {
            @Nullable
            @Override
            public String apply(@Nullable Long cuboid) {
                return Long.toString(cuboid);
            }
        }));
    }
}