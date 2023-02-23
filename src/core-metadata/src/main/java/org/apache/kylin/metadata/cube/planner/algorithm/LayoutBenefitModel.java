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

package org.apache.kylin.metadata.cube.planner.algorithm;

import java.math.BigInteger;

public class LayoutBenefitModel {
    private LayoutModel layoutModel;
    private BenefitModel benefitModel;

    public LayoutBenefitModel(LayoutModel layoutModel, BenefitModel benefitModel) {
        this.layoutModel = layoutModel;
        this.benefitModel = benefitModel;
    }

    public void reset(LayoutModel layoutModel, BenefitModel benefitModel) {
        this.layoutModel = layoutModel;
        this.benefitModel = benefitModel;
    }

    public BigInteger getLayoutId() {
        return layoutModel == null ? null : layoutModel.layoutId;
    }

    public Double getBenefit() {
        return benefitModel == null ? null : benefitModel.benefit;
    }

    @Override
    public String toString() {
        return "LayoutBenefitModel [layoutModel=" + layoutModel + ", benefitModel=" + benefitModel + "]";
    }

    public static class LayoutModel {
        public final BigInteger layoutId;

        public final long recordCount;
        public final double spaceSize;

        public final double hitProb;
        public final long scanCount;

        public LayoutModel(BigInteger layoutId, long recordCount, double spaceSize, double hitProb, long scanCount) {
            this.layoutId = layoutId;
            this.recordCount = recordCount;
            this.spaceSize = spaceSize;
            this.hitProb = hitProb;
            this.scanCount = scanCount;
        }

        @Override
        public String toString() {
            return "LayoutModel [layoutId=" + layoutId + ", recordCount=" + recordCount + ", spaceSize=" + spaceSize
                    + ", hitProbability=" + hitProb + ", scanCount=" + scanCount + "]";
        }
    }

    public static class BenefitModel {
        public final double benefit;
        public final int benefitCount;

        public BenefitModel(double benefit, int benefitCount) {
            this.benefit = benefit;
            this.benefitCount = benefitCount;
        }

        @Override
        public String toString() {
            return "BenefitModel [benefit=" + benefit + ", benefitCount=" + benefitCount + "]";
        }
    }
}
