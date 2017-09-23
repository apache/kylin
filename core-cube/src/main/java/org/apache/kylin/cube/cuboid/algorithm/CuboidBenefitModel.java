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
        return cuboidModel == null ? null : cuboidModel.getCuboidId();
    }

    public Double getBenefit() {
        return benefitModel == null ? null : benefitModel.getBenefit();
    }

    @Override
    public String toString() {
        return "CuboidBenefitModel [cuboidModel=" + cuboidModel + ", benefitModel=" + benefitModel + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((cuboidModel == null) ? 0 : cuboidModel.hashCode());
        result = prime * result + ((benefitModel == null) ? 0 : benefitModel.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        CuboidBenefitModel other = (CuboidBenefitModel) obj;
        if (cuboidModel == null) {
            if (other.cuboidModel != null)
                return false;
        } else if (!cuboidModel.equals(other.cuboidModel))
            return false;
        if (benefitModel == null) {
            if (other.benefitModel != null)
                return false;
        } else if (!benefitModel.equals(other.benefitModel))
            return false;
        return true;
    }

    public static class CuboidModel {
        private long cuboidId;

        private long recordCount;
        private double spaceSize;

        private double hitProbability;
        private long scanCount;

        public CuboidModel(long cuboId, long recordCount, double spaceSize, double hitProbability, long scanCount) {
            this.cuboidId = cuboId;
            this.recordCount = recordCount;
            this.spaceSize = spaceSize;
            this.hitProbability = hitProbability;
            this.scanCount = scanCount;
        }

        public long getCuboidId() {
            return cuboidId;
        }

        public long getRecordCount() {
            return recordCount;
        }

        public double getSpaceSize() {
            return spaceSize;
        }

        public double getHitProbability() {
            return hitProbability;
        }

        public long getScanCount() {
            return scanCount;
        }

        @Override
        public String toString() {
            return "CuboidModel [cuboidId=" + cuboidId + ", recordCount=" + recordCount + ", spaceSize=" + spaceSize
                    + ", hitProbability=" + hitProbability + ", scanCount=" + scanCount + "]";
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + (int) (cuboidId ^ (cuboidId >>> 32));
            result = prime * result + (int) (recordCount ^ (recordCount >>> 32));
            long temp;
            temp = Double.doubleToLongBits(spaceSize);
            result = prime * result + (int) (temp ^ (temp >>> 32));
            temp = Double.doubleToLongBits(hitProbability);
            result = prime * result + (int) (temp ^ (temp >>> 32));
            result = prime * result + (int) (scanCount ^ (scanCount >>> 32));
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;

            CuboidModel other = (CuboidModel) obj;
            if (cuboidId != other.cuboidId)
                return false;
            if (recordCount != other.recordCount)
                return false;
            if (Double.doubleToLongBits(spaceSize) != Double.doubleToLongBits(other.spaceSize))
                return false;
            if (hitProbability != other.hitProbability)
                return false;
            if (scanCount != other.scanCount)
                return false;
            return true;
        }
    }

    public static class BenefitModel {
        private double benefit;
        private int benefitCount;

        public BenefitModel(double benefit, int benefitCount) {
            this.benefit = benefit;
            this.benefitCount = benefitCount;
        }

        public double getBenefit() {
            return benefit;
        }

        public int getBenefitCount() {
            return benefitCount;
        }

        @Override
        public String toString() {
            return "BenefitModel [benefit=" + benefit + ", benefitCount=" + benefitCount + "]";
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            long temp;
            temp = Double.doubleToLongBits(benefit);
            result = prime * result + (int) (temp ^ (temp >>> 32));
            result = prime * result + benefitCount;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            BenefitModel other = (BenefitModel) obj;
            if (Double.doubleToLongBits(benefit) != Double.doubleToLongBits(other.benefit))
                return false;
            if (benefitCount != other.benefitCount)
                return false;
            return true;
        }
    }
}
