#
# /*
#  * Licensed to the Apache Software Foundation (ASF) under one
#  * or more contributor license agreements.  See the NOTICE file
#  * distributed with this work for additional information
#  * regarding copyright ownership.  The ASF licenses this file
#  * to you under the Apache License, Version 2.0 (the
#  * "License"); you may not use this file except in compliance
#  * with the License.  You may obtain a copy of the License at
#  *
#  *     http://www.apache.org/licenses/LICENSE-2.0
#  *
#  * Unless required by applicable law or agreed to in writing, software
#  * distributed under the License is distributed on an "AS IS" BASIS,
#  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  * See the License for the specific language governing permissions and
#  * limitations under the License.
#  */
#
package_version="2.9"

package_x64="async-profiler-${package_version}-linux-x64"
package_arm64="async-profiler-${package_version}-linux-arm64"

file_suffix=".tar.gz"
package_full_x64="${package_x64}${file_suffix}"
package_full_arm64="${package_arm64}${file_suffix}"

wget https://github.com/jvm-profiling-tools/async-profiler/releases/download/v${package_version}/${package_full_x64}
wget https://github.com/jvm-profiling-tools/async-profiler/releases/download/v${package_version}/${package_full_arm64}

if [ `md5sum ${package_full_x64} | awk '{print $1}'` != "29127cee36b7acf069d31603b4558361" ]
then
    echo "md5 check failed for ${package_full_x64}"
    exit 1
fi

if [ `md5sum ${package_full_arm64} | awk '{print $1}'` != "d31a70d2c176146a46dffc15948040ed" ]
then
    echo "md5 check failed for ${package_full_arm64}"
    exit 1
fi

tar -zxf ${package_full_x64}
tar -zxf ${package_full_arm64}

cp ${package_x64}/build/libasyncProfiler.so libasyncProfiler-linux-x64.so
cp ${package_arm64}/build/libasyncProfiler.so libasyncProfiler-linux-arm64.so

rm -rf ${package_full_x64}
rm -rf ${package_full_arm64}
rm -rf ${package_x64}
rm -rf ${package_arm64}