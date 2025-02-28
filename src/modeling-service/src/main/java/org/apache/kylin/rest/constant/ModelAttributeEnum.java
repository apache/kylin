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

package org.apache.kylin.rest.constant;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.metadata.model.NDataModel;

public enum ModelAttributeEnum {

    STREAMING {
        @Override
        public List<NDataModel> filter(List<NDataModel> models) {
            return models.stream() //
                    .filter(model -> model.getModelType() == NDataModel.ModelType.STREAMING) //
                    .collect(Collectors.toList());
        }
    },

    BATCH {
        @Override
        public List<NDataModel> filter(List<NDataModel> models) {
            return models.stream() //
                    .filter(model -> model.getModelType() == NDataModel.ModelType.BATCH) //
                    .collect(Collectors.toList());
        }
    },

    HYBRID {
        @Override
        public List<NDataModel> filter(List<NDataModel> models) {
            return models.stream() //
                    .filter(model -> model.getModelType() == NDataModel.ModelType.HYBRID) //
                    .collect(Collectors.toList());
        }
    };

    public abstract List<NDataModel> filter(List<NDataModel> models);

}
