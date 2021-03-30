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

package org.apache.kylin.cube.model;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.SourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.ImmutableList;

public class CubeDescTiretreeGlobalDomainDictUtil {
    private static final Logger logger = LoggerFactory.getLogger(CubeDescTiretreeGlobalDomainDictUtil.class);

    /**
     * get reuse global tiretree global dic path
     * @param tblColRef
     * @param cubeDesc
     * @return
     */
    public static String globalReuseDictPath(KylinConfig config, TblColRef tblColRef, CubeDesc cubeDesc) {
        String globalResumeDictPath = null;
        List<GlobalDict> globalDicts = cubeDesc.listDomainDict();
        DataModelManager metadataManager = DataModelManager.getInstance(config);
        CubeManager cubeManager = CubeManager.getInstance(config);
        for (GlobalDict dict : globalDicts) {
            if (dict.getSrc().getIdentity().equalsIgnoreCase(tblColRef.getIdentity())) {
                String model = dict.getModel();
                String cube = dict.getCube();
                logger.info("cube:{} column:{} tiretree global domain dic reuse model:{} cube{} column:{} ",
                        cubeDesc.getName(), tblColRef.getName(), model, cube, dict.getDesc());

                DataModelDesc dataModel = metadataManager.getDataModelDesc(model);
                if (Objects.isNull(dataModel)) {
                    logger.error("get cube:{} column:{} tiretree global domain dic reuse DataModelDesc error",
                            cubeDesc.getName(), tblColRef.getName());
                    return null;
                }

                CubeInstance cubeInstance = cubeManager.getCube(cube);
                CubeSegment cubeSegment = cubeInstance.getLatestReadySegment();

                TblColRef colRef = dataModel.findColumn(dict.getDesc());
                if (Objects.isNull(colRef)) {
                    logger.error("get cube:{} column:{} tiretree global domain dic TblColRef error");
                    return null;
                }

                globalResumeDictPath = cubeSegment.getDictResPath(colRef);

                if (StringUtils.isBlank(globalResumeDictPath)) {
                    logger.error("get cube:{} column:{} tiretree global domain dic resume dict path error");
                }
                logger.error("get cube:{} column:{} tiretree global domain dic resume dict path is {}",
                        globalResumeDictPath);
                break;
            }
        }
        return globalResumeDictPath;
    }

    /**
     * add resuce global tiretree global dic for baseid job
     * @param cubeDesc
     * @param dumpList
     */
    public static void cuboidJob(CubeDesc cubeDesc, Set<String> dumpList) {
        logger.info("cube {} start to add global domain dic", cubeDesc.getName());
        CubeManager cubeManager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        DataModelManager metadataManager = DataModelManager.getInstance(KylinConfig.getInstanceFromEnv());

        cubeManager.getCube(cubeDesc.getName());
        List<GlobalDict> globalDicts = cubeDesc.listDomainDict();

        for (GlobalDict dict : globalDicts) {
            String cube = dict.getCube();
            String model = dict.getModel();
            logger.debug("cube {} column {} start to add global domain dic ,reuse {}.{}.{}", cubeDesc.getName(),
                    dict.getSrc(), model, cube, dict.getDesc());
            CubeInstance instance = cubeManager.getCube(cube);
            logger.debug("cube {} column {} start to add global domain dic ,reuse cube{} dict", cubeDesc.getName(),
                    dict.getSrc(), instance.getName());

            // cube, model_desc, cube_desc, table
            dumpList.add(instance.getResourcePath());
            dumpList.add(instance.getDescriptor().getModel().getResourcePath());
            dumpList.add(instance.getDescriptor().getResourcePath());
            dumpList.add(instance.getProjectInstance().getResourcePath());

            for (TableRef tableRef : instance.getDescriptor().getModel().getAllTables()) {
                TableDesc table = tableRef.getTableDesc();
                dumpList.add(table.getResourcePath());
                dumpList.addAll(SourceManager.getMRDependentResources(table));
            }

            DataModelDesc dataModelDesc = metadataManager.getDataModelDesc(model);
            logger.debug("cube {} column {} start to add global domain dic ,reuse model{} dict", cubeDesc.getName(),
                    dict.getSrc(), dataModelDesc.getName());
            TblColRef tblColRef = dataModelDesc.findColumn(dict.getDesc());
            CubeSegment segment = instance.getLatestReadySegment();
            logger.debug(
                    "cube {} column {} start to add global domain dic ,reuse mode:{} cube:{} segment:{} dict,tblColRef:{}",
                    cubeDesc.getName(), dict.getSrc(), dataModelDesc.getName(), cube, segment.getName(),
                    tblColRef.getIdentity());
            if (segment.getDictResPath(tblColRef) != null) {
                dumpList.addAll(ImmutableList.of(segment.getDictResPath(tblColRef)));
            }
        }
    }

    public static class GlobalDict implements Serializable {
        private TblColRef src;
        private String desc;
        private String cube;
        private String model;

        public GlobalDict(TblColRef src, String desc, String cube, String model) {
            this.src = src;
            this.desc = desc;
            this.cube = cube;
            this.model = model;
        }

        public TblColRef getSrc() {
            return src;
        }

        public String getDesc() {
            return desc;
        }

        public String getCube() {
            return cube;
        }

        public String getModel() {
            return model;
        }
    }
}
