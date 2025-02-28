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

package org.apache.kylin.metadata.recommendation.ref;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.metadata.model.NDataModel;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class ModelColumnRef extends RecommendationRef {

    public ModelColumnRef(NDataModel.NamedColumn column, String dataType, String content) {
        this.setId(column.getId());
        this.setName(column.getAliasDotColumn());
        this.setContent(content);
        this.setDataType(dataType);
        this.setExisted(true);
        this.setEntity(column);
    }

    public NDataModel.NamedColumn getColumn() {
        Preconditions.checkArgument(getEntity() instanceof NDataModel.NamedColumn);
        return (NDataModel.NamedColumn) getEntity();
    }

    @Override
    public void rebuild(String newName) {
        throw new IllegalStateException("Rebuild ModelColumnRef is not allowed.");
    }
}
