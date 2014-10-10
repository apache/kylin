package com.kylinolap.dict.lookup;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.kylinolap.common.persistence.Serializer;
import com.kylinolap.common.util.JsonUtil;

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

/**
 * @author yangli9
 * 
 */
public class SnapshotTableSerializer implements Serializer<SnapshotTable> {

	public static final SnapshotTableSerializer FULL_SERIALIZER = new SnapshotTableSerializer(
			false);
	public static final SnapshotTableSerializer INFO_SERIALIZER = new SnapshotTableSerializer(
			true);

	private boolean infoOnly;

	SnapshotTableSerializer(boolean infoOnly) {
		this.infoOnly = infoOnly;
	}

	@Override
	public void serialize(SnapshotTable obj, DataOutputStream out)
			throws IOException {
		String json = JsonUtil.writeValueAsIndentString(obj);
		out.writeUTF(json);

		if (infoOnly == false)
			obj.writeData(out);
	}

	@Override
	public SnapshotTable deserialize(DataInputStream in) throws IOException {
		String json = in.readUTF();
		SnapshotTable obj = JsonUtil.readValue(json, SnapshotTable.class);

		if (infoOnly == false)
			obj.readData(in);

		return obj;
	}

}
