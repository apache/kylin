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

import java.util.List;
import java.util.Objects;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.NDataModel;

import com.google.common.base.Preconditions;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class MeasureRef extends RecommendationRef {

    public MeasureRef(MeasureDesc measure, int id, boolean existed) {
        this.setId(id);
        this.setEntity(measure);
        this.setContent(JsonUtil.writeValueAsStringQuietly(measure));
        this.setName(measure.getName());
        this.setExisted(existed);
    }

    @Override
    public void rebuild(String userDefinedName) {
        NDataModel.Measure measure = this.getMeasure();
        measure.setName(userDefinedName);
        final FunctionDesc function = measure.getFunction();
        final List<ParameterDesc> parameters = function.getParameters();
        final List<RecommendationRef> dependencies = this.getDependencies();
        Preconditions.checkArgument(parameters.size() == dependencies.size());
        for (int i = 0; i < dependencies.size(); i++) {
            parameters.get(i).setValue(dependencies.get(i).getName());
        }
        this.setEntity(measure);
        this.setContent(JsonUtil.writeValueAsStringQuietly(measure));
        this.setName(measure.getName());
    }

    public NDataModel.Measure getMeasure() {
        return (NDataModel.Measure) getEntity();
    }

    @Override
    public String getDataType() {
        return this.getMeasure().getFunction().getReturnType();
    }

    private boolean isDependenciesIdentical(MeasureRef measureRef) {
        return Objects.equals(this.getDependencies(), measureRef.getDependencies());
    }

    private boolean isFunctionIdentical(MeasureRef measureRef) {
        return Objects.equals(this.getMeasure().getFunction().getExpression(),
                measureRef.getMeasure().getFunction().getExpression());
    }

    public boolean isIdentical(RecommendationRef ref) {
        if (ref == null) {
            return false;
        }
        Preconditions.checkArgument(ref instanceof MeasureRef);
        MeasureRef measureRef = (MeasureRef) ref;
        return isFunctionIdentical(measureRef) && isDependenciesIdentical(measureRef);
    }
}
