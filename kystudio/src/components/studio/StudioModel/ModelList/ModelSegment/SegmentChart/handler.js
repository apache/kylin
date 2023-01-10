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
const MILLISECOND = 'millisecond'
const SECOND = 'second'
const MINUTE = 'minute'
const HOUR = 'hour'
const DAY = 'day'
const MONTH = 'month'
const YEAR = 'year'

export const scaleTypes = {
  MILLISECOND,
  SECOND,
  MINUTE,
  HOUR,
  DAY,
  MONTH,
  YEAR
}

export const formatTypes = {
  [HOUR]: 'HH:mm A',
  [DAY]: 'MMM DD',
  [MONTH]: 'MMM',
  [YEAR]: 'YYYY'
}

export function isFilteredSegmentsContinue (segment, selectedSegments) {
  const isSegmentSelected = selectedSegments.some(selectedSegment => selectedSegment.id === segment.id)
  const filteredSegments = isSegmentSelected ? selectedSegments.filter(selectSegment => segment.id !== selectSegment.id) : [...selectedSegments, segment]

  filteredSegments.sort((segmentA, segmentB) => segmentA.startTime < segmentB.startTime ? -1 : 1)

  return filteredSegments.every((currentSegment, index) => {
    const lastSegment = filteredSegments[index - 1]

    return lastSegment ? lastSegment.endTime === currentSegment.startTime : true
  })
}
