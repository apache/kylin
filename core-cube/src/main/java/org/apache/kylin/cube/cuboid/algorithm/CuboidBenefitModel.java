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

package org.apache.kylin.cube.cuboid.algorithm;

public class CuboidBenefitModel {
    private CuboidModel cuboidModel;
    private BenefitModel benefitModel;

    public CuboidBenefitModel(CuboidModel cuboidModel, BenefitModel benefitModel) {
        this.cuboidModel = cuboidModel;
        this.benefitModel = benefitModel;
    }

    public void reset(CuboidModel cuboidModel, BenefitModel benefitModel) {
        this.cuboidModel = cuboidModel;
        this.benefitModel = benefitModel;
    }

    public Long getCuboidId() {
        return cuboidModel == null ? null : cuboidModel.cuboidId;
    }

    public Double getBenefit() {
        return benefitModel == null ? null : benefitModel.benefit;
    }

    @Override
    public String toString() {
        return "CuboidBenefitModel [cuboidModel=" + cuboidModel + ", benefitModel=" + benefitModel + "]";
    }

    public static class CuboidModel {
        public final long cuboidId;

        public final long recordCount;
        public final double spaceSize;

        public final double hitProbability;
        public final long scanCount;

        public CuboidModel(long cuboId, long recordCount, double spaceSize, double hitProbability, long scanCount) {
            this.cuboidId = cuboId;
            this.recordCount = recordCount;
            this.spaceSize = spaceSize;
            this.hitProbability = hitProbability;
            this.scanCount = scanCount;
        }

        @Override
        public String toString() {
            return "CuboidModel [cuboidId=" + cuboidId + ", recordCount=" + recordCount + ", spaceSize=" + spaceSize
                    + ", hitProbability=" + hitProbability + ", scanCount=" + scanCount + "]";
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