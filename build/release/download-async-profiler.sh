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

if [ ! -f "build/${package_full_x64}" ]
then
    echo "No ${package_x64} binary file found."
    wget --directory-prefix=build/ https://github.com/async-profiler/async-profiler/releases/download/v${package_version}/${package_full_x64} || { echo "Download ${package_x64} failed." && exit 1; }
else
    if [ `md5sum build/${package_full_x64} | awk '{print $1}'` != "29127cee36b7acf069d31603b4558361" ]
    then
        echo "${package_x64} md5 check failed."
        rm build/${package_full_x64}
        wget --directory-prefix=build/ https://github.com/async-profiler/async-profiler/releases/download/v${package_version}/${package_full_x64} || { echo "Download ${package_x64} failed." && exit 1; }
    fi
fi

if [ ! -f "build/${package_full_arm64}" ]
then
    echo "No ${package_x64} binary file found."
    wget --directory-prefix=build/ https://github.com/async-profiler/async-profiler/releases/download/v${package_version}/${package_full_arm64} || { echo "Download ${package_full_arm64} failed." && exit 1; }
else
    if [ `md5sum build/${package_full_arm64} | awk '{print $1}'` != "d31a70d2c176146a46dffc15948040ed" ]
    then
        echo "${package_x64} md5 check failed."
        rm build/${package_full_arm64}
        wget --directory-prefix=build/ https://github.com/async-profiler/async-profiler/releases/download/v${package_version}/${package_full_arm64} || { echo "Download ${package_full_arm64} failed." && exit 1; }
    fi
fi

rm -rf build/async-profiler
mkdir -p build/async-profiler
tar -zxf build/${package_full_x64} -C build/async-profiler/
tar -zxf build/${package_full_arm64} -C build/async-profiler/

cp build/async-profiler/${package_x64}/build/libasyncProfiler.so build/libasyncProfiler-linux-x64.so
cp build/async-profiler/${package_arm64}/build/libasyncProfiler.so build/libasyncProfiler-linux-arm64.so
