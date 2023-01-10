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
export function formatSegments (that, segments) {
  return segments.map(segment => {
    const isFullLoad = segment.segRange.date_range_start === 0 && segment.segRange.date_range_end === 9223372036854776000
    return {
      ...segment,
      segmentPath: segment.additionalInfo && segment.additionalInfo.segment_path,
      fileNumber: segment.additionalInfo && segment.additionalInfo.file_count,
      startTime: isFullLoad ? that.$t('fullLoad') : segment.segRange.date_range_start,
      endTime: isFullLoad ? that.$t('fullLoad') : segment.segRange.date_range_end
    }
  })
}
export function formatStreamSegments (that, segments) {
  return segments.map(segment => {
    const isFullLoad = segment.segRange.source_offset_start === 0 && segment.segRange.source_offset_end === 9223372036854776000
    return {
      ...segment,
      segmentPath: segment.additionalInfo && segment.additionalInfo.segment_path,
      fileNumber: segment.additionalInfo && segment.additionalInfo.file_count,
      startTime: isFullLoad ? that.$t('fullLoad') : segment.segRange.source_offset_start,
      endTime: isFullLoad ? that.$t('fullLoad') : segment.segRange.source_offset_end
    }
  })
}
