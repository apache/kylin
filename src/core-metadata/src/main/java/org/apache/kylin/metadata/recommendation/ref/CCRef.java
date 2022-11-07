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

import org.apache.kylin.metadata.model.ComputedColumnDesc;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class CCRef extends RecommendationRef {

    public CCRef(ComputedColumnDesc cc, int id) {
        this.setId(id);
        this.setEntity(cc);
        this.setName(cc.getFullName());
        this.setContent(cc.getExpression());
        this.setDataType(cc.getDatatype());
    }

    public ComputedColumnDesc getCc() {
        return (ComputedColumnDesc) getEntity();
    }

    @Override
    public void rebuild(String newName) {
        ComputedColumnDesc cc = getCc();
        cc.setColumnName(newName);
        this.setName(cc.getFullName());
    }

    public boolean isIdentical(CCRef anotherCC) {
        if (anotherCC == null) {
            return false;
        }
        ComputedColumnDesc thisCC = this.getCc();
        ComputedColumnDesc thatCC = anotherCC.getCc();

        return thisCC.getFullName().equalsIgnoreCase(thatCC.getFullName())
                && thisCC.getDatatype().equalsIgnoreCase(thatCC.getDatatype())
                && thisCC.getInnerExpression().equalsIgnoreCase(thatCC.getInnerExpression());
    }
}
