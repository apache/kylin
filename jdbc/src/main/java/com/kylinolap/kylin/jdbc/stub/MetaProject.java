/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.kylin.jdbc.stub;

import com.kylinolap.kylin.jdbc.KylinMetaImpl.MetaCatalog;
import com.kylinolap.kylin.jdbc.KylinMetaImpl.MetaColumn;
import com.kylinolap.kylin.jdbc.KylinMetaImpl.MetaSchema;
import com.kylinolap.kylin.jdbc.KylinMetaImpl.MetaTable;

/**
 * @author xduo
 * 
 */
public class MetaProject {

	private final DataSet<MetaCatalog> metaCatalogs;

	private final DataSet<MetaSchema> metaSchemas;

	private final DataSet<MetaTable> metaTables;

	private final DataSet<MetaColumn> metaColumns;

	public MetaProject(DataSet<MetaCatalog> metaCatalogs,
			DataSet<MetaSchema> metaSchemas, DataSet<MetaTable> metaTables,
			DataSet<MetaColumn> metaColumns) {
		this.metaCatalogs = metaCatalogs;
		this.metaSchemas = metaSchemas;
		this.metaTables = metaTables;
		this.metaColumns = metaColumns;
	}

	public DataSet<MetaCatalog> getMetaCatalogs() {
		return metaCatalogs;
	}

	public DataSet<MetaSchema> getMetaSchemas() {
		return metaSchemas;
	}

	public DataSet<MetaTable> getMetaTables() {
		return metaTables;
	}

	public DataSet<MetaColumn> getMetaColumns() {
		return metaColumns;
	}

	@Override
	public String toString() {
		return "MetaProject [metaSchemas=" + metaSchemas + "]";
	}

}
