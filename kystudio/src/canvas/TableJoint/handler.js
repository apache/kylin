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

function getPoints (position, node) {
  switch (position) {
    case 'top': return { x: node.x + node.width / 2, y: node.y }
    case 'right': return { x: node.x + node.width, y: node.y + node.height / 2 }
    case 'bottom': return { x: node.x + node.width / 2, y: node.y + node.height }
    case 'left': return { x: node.x, y: node.y + node.height / 2 }
    default: return { x: node.x, y: node.y }
  }
}

function getDistanceBetween (sourcePoint, targetPoint) {
  return Math.sqrt(
    Math.pow((targetPoint.y - sourcePoint.y), 2) +
    Math.pow((targetPoint.x - sourcePoint.x), 2)
  )
}

function getShortestPointPair (sourcePoints = [], targetPoints = []) {
  let minDistance = Infinity
  let source = null
  let target = null

  for (const sourcePoint of sourcePoints) {
    for (const targetPoint of targetPoints) {
      const distance = getDistanceBetween(sourcePoint, targetPoint)
      if (minDistance > distance) {
        minDistance = distance
        source = sourcePoint
        target = targetPoint
      }
    }
  }

  return { source, target }
}

export function getConnectorPoints (sourceSize, targetSize) {
  const sourcePoints = ['bottom'].map(position => getPoints(position, sourceSize))
  const targetPoints = ['top'].map(position => getPoints(position, targetSize))

  return getShortestPointPair(sourcePoints, targetPoints)
}

export function getCenterPoint (source, target) {
  const x = source.x + (target.x - source.x) / 2
  const y = source.y + (target.y - source.y) / 2
  return { x, y }
}
