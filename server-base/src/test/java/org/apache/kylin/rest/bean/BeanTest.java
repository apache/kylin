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

package org.apache.kylin.rest.bean;

import java.beans.IntrospectionException;

import org.apache.kylin.metadata.querymeta.ColumnMeta;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.metadata.querymeta.TableMeta;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.AccessRequest;
import org.apache.kylin.rest.request.CubeRequest;
import org.apache.kylin.rest.request.JobListRequest;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.AccessEntryResponse;
import org.apache.kylin.rest.response.SQLResponse;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author xduo
 * 
 */
public class BeanTest {

    @Test
    public void test() {
        try {
            BeanValidator.validateAccssor(ColumnMeta.class, new String[0]);
            BeanValidator.validateAccssor(TableMeta.class, new String[0]);
            BeanValidator.validateAccssor(SelectedColumnMeta.class, new String[0]);
            BeanValidator.validateAccssor(AccessRequest.class, new String[0]);
            BeanValidator.validateAccssor(CubeRequest.class, new String[0]);
            BeanValidator.validateAccssor(JobListRequest.class, new String[0]);
            BeanValidator.validateAccssor(SQLRequest.class, new String[0]);
            BeanValidator.validateAccssor(AccessEntryResponse.class, new String[0]);
            BeanValidator.validateAccssor(SQLResponse.class, new String[0]);
        } catch (IntrospectionException e) {
        }

        new SQLResponse(null, null, 0, true, null);

        SelectedColumnMeta coulmnMeta = new SelectedColumnMeta(false, false, false, false, 0, false, 0, null, null,
                null, null, null, 0, 0, 0, null, false, false, false);
        Assert.assertTrue(!coulmnMeta.isAutoIncrement());
        Assert.assertTrue(!coulmnMeta.isCaseSensitive());
        Assert.assertTrue(!coulmnMeta.isSearchable());
        Assert.assertTrue(!coulmnMeta.isCurrency());
        Assert.assertTrue(coulmnMeta.getIsNullable() == 0);
        Assert.assertTrue(!coulmnMeta.isSigned());

        Assert.assertEquals(Constant.ACCESS_HAS_ROLE_ADMIN, "hasRole('ROLE_ADMIN')");
        Assert.assertEquals(Constant.ACCESS_POST_FILTER_READ,
                "hasRole('ROLE_ADMIN') " + " or hasPermission(filterObject, 'ADMINISTRATION')"
                        + " or hasPermission(filterObject, 'MANAGEMENT')"
                        + " or hasPermission(filterObject, 'OPERATION')" + " or hasPermission(filterObject, 'READ')");
        Assert.assertEquals(Constant.FakeCatalogName, "defaultCatalog");
        Assert.assertEquals(Constant.FakeSchemaName, "defaultSchema");
        Assert.assertEquals(Constant.IDENTITY_ROLE, "role");
        Assert.assertEquals(Constant.IDENTITY_USER, "user");
    }
}
