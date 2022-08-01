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

package org.apache.kylin.metadata.querymeta;

import java.io.Serializable;
import java.util.HashSet;

import lombok.NoArgsConstructor;

/**
 * Created by luwei on 17-4-26.
 */
@SuppressWarnings("serial")
@NoArgsConstructor
public class TableMetaWithType extends TableMeta {
    public static enum tableTypeEnum implements Serializable {

        LOOKUP, FACT

    }

    private HashSet<tableTypeEnum> TYPE;

    public TableMetaWithType(String tABLE_CAT, String tABLE_SCHEM, String tABLE_NAME, String tABLE_TYPE, String rEMARKS,
            String tYPE_CAT, String tYPE_SCHEM, String tYPE_NAME, String sELF_REFERENCING_COL_NAME,
            String rEF_GENERATION) {
        TABLE_CAT = tABLE_CAT;
        TABLE_SCHEM = tABLE_SCHEM;
        TABLE_NAME = tABLE_NAME;
        TABLE_TYPE = tABLE_TYPE;
        REMARKS = rEMARKS;
        TYPE_CAT = tYPE_CAT;
        TYPE_SCHEM = tYPE_SCHEM;
        TYPE_NAME = tYPE_NAME;
        SELF_REFERENCING_COL_NAME = sELF_REFERENCING_COL_NAME;
        REF_GENERATION = rEF_GENERATION;
        TYPE = new HashSet<tableTypeEnum>();
    }

    public HashSet<tableTypeEnum> getTYPE() {
        return TYPE;
    }

    public void setTYPE(HashSet<tableTypeEnum> TYPE) {
        this.TYPE = TYPE;
    }
}
