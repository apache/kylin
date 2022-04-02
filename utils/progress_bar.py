#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import sys
import threading
from datetime import datetime


class ProgressBar(object):

    def __init__(self, filename):
        self._filename = filename
        self._size = int(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()
        self._done = 0
        self._start = datetime.now()
        self._end = None

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._end = datetime.now()
            self._seen_so_far += bytes_amount
            self._done = int(50 * self._seen_so_far / self._size)
            sys.stdout.write(f"\r[{'=' * self._done}{' ' * (50 - self._done)}] % {self._seen_so_far} / {self._size} "
                             f"- Duration: {self._end - self._start}\r")
            sys.stdout.flush()
