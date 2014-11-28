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

package com.kylinolap.cube.model.validation.rule;

import com.kylinolap.metadata.MetadataConstances;

/**
 * @author jianliu
 * 
 */
public interface IKylinValidationConstances extends MetadataConstances {

    public static final int DEFAULT_MAX_AGR_GROUP_SIZE = 20;
    public static final String KEY_MAX_AGR_GROUP_SIZE = "rule_max.arggregation.group.size";
    public static final String KEY_IGNORE_UNKNOWN_FUNC = "rule_ignore_unknown_func";

}