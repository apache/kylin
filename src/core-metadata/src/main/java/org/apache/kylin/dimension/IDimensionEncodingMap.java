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

package org.apache.kylin.dimension;

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.metadata.model.TblColRef;

/**
 * Dimension encoding maps a dimension (String) to bytes of fixed length.
 *
 * It is similar to Dictionary in 1) the bytes is fixed length; 2) bi-way mapping;
 * 3) the mapping preserves order, but is also different to Dictionary as the target
 * bytes can be very long while dictionary ID is 4 bytes at most. This means it is
 * hard to enumerate all values of a encoding, thus TupleFilterDictionaryTranslater
 * cannot work on DimensionEncoding.
 */
public interface IDimensionEncodingMap {

    /** Get dimension encoding of a column */
    DimensionEncoding get(TblColRef col);

    /** Get dictionary of a column if its encoding is dictionary based */
    Dictionary<String> getDictionary(TblColRef col);

}
