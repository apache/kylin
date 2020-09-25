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

package org.apache.kylin.rest.signature;

import java.util.List;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.storage.hybrid.HybridInstance;
import org.apache.kylin.storage.hybrid.HybridManager;

import org.apache.kylin.shaded.com.google.common.collect.Sets;

public abstract class RealizationSignature extends ComponentSignature<RealizationSignature> {

    static class CubeSignature extends RealizationSignature {
        public final String name;
        public final RealizationStatusEnum status;
        public final Set<SegmentSignature> segmentSignatureSet;

        private CubeSignature(String name, RealizationStatusEnum status, Set<SegmentSignature> segmentSignatureSet) {
            this.name = name;
            this.status = status;
            this.segmentSignatureSet = segmentSignatureSet;
        }

        public String getKey() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            CubeSignature that = (CubeSignature) o;

            if (name != null ? !name.equals(that.name) : that.name != null)
                return false;
            if (status != that.status)
                return false;
            return segmentSignatureSet != null ? segmentSignatureSet.equals(that.segmentSignatureSet)
                    : that.segmentSignatureSet == null;
        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (status != null ? status.hashCode() : 0);
            result = 31 * result + (segmentSignatureSet != null ? segmentSignatureSet.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return name + "-" + status + ":"
                    + (segmentSignatureSet != null ? Sets.newTreeSet(segmentSignatureSet) : null);
        }

        static CubeSignature getCubeSignature(KylinConfig config, String realizationName) {
            CubeInstance cubeInstance = CubeManager.getInstance(config).getCube(realizationName);
            if (cubeInstance == null) {
                return null;
            }
            if (!cubeInstance.isReady()) {
                return new CubeSignature(realizationName, RealizationStatusEnum.DISABLED, null);
            }
            List<CubeSegment> readySegments = cubeInstance.getSegments(SegmentStatusEnum.READY);
            Set<SegmentSignature> segmentSignatureSet = Sets.newHashSetWithExpectedSize(readySegments.size());
            for (CubeSegment cubeSeg : readySegments) {
                segmentSignatureSet.add(new SegmentSignature(cubeSeg.getName(), cubeSeg.getLastBuildTime()));
            }
            return new CubeSignature(realizationName, RealizationStatusEnum.READY, segmentSignatureSet);
        }
    }

    static class HybridSignature extends RealizationSignature {
        public final String name;
        public final Set<RealizationSignature> realizationSignatureSet;

        private HybridSignature(String name, Set<RealizationSignature> realizationSignatureSet) {
            this.name = name;
            this.realizationSignatureSet = realizationSignatureSet;
        }

        public String getKey() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            HybridSignature that = (HybridSignature) o;

            if (name != null ? !name.equals(that.name) : that.name != null)
                return false;
            return realizationSignatureSet != null ? realizationSignatureSet.equals(that.realizationSignatureSet)
                    : that.realizationSignatureSet == null;

        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (realizationSignatureSet != null ? realizationSignatureSet.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return name + ":" + (realizationSignatureSet != null ? Sets.newTreeSet(realizationSignatureSet) : null);
        }

        static HybridSignature getHybridSignature(KylinConfig config, String realizationName) {
            HybridInstance hybridInstance = HybridManager.getInstance(config).getHybridInstance(realizationName);
            if (hybridInstance == null) {
                return null;
            }
            IRealization[] realizations = hybridInstance.getRealizations();
            Set<RealizationSignature> realizationSignatureSet = Sets.newHashSetWithExpectedSize(realizations.length);
            for (IRealization realization : realizations) {
                RealizationSignature realizationSignature = null;
                if (realization.getType() == RealizationType.CUBE) {
                    realizationSignature = CubeSignature.getCubeSignature(config, realization.getName());
                } else if (realization.getType() == RealizationType.HYBRID) {
                    realizationSignature = getHybridSignature(config, realization.getName());
                }
                if (realizationSignature != null) {
                    realizationSignatureSet.add(realizationSignature);
                }
            }
            return new HybridSignature(realizationName, realizationSignatureSet);
        }
    }
}
