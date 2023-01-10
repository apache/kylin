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
import emptyCuboidsUrl from './empty.png'
import brokenCuboidsUrl from './broken.png'

export const backgroundMaps = {
  BROKEN: {
    url: brokenCuboidsUrl,
    width: 60,
    height: 24
  },
  EMPTY: {
    url: emptyCuboidsUrl,
    width: 60,
    height: 18
  }
}

export function formatGraphData (data) {
  return [
    {
      name: 'AUTO_AGG',
      value: data['auto_agg_indexes'].total_size,
      children: data['auto_agg_indexes'].indexes.filter((k) => {
        return k.data_size > 0
      }).map((i) => {
        return { name: i.id, value: i.data_size, usage: i.usage }
      })
    },
    {
      name: 'AUTO_TABLE',
      value: data['auto_table_indexes'].total_size,
      children: data['auto_table_indexes'].indexes.filter((k) => {
        return k.data_size > 0
      }).map((i) => {
        return { name: i.id, value: i.data_size, usage: i.usage }
      })
    },
    {
      name: 'MANUAL_AGG',
      value: data['manual_agg_indexes'].total_size,
      children: data['manual_agg_indexes'].indexes.filter((k) => {
        return k.data_size > 0
      }).map((i) => {
        return { name: i.id, value: i.data_size, usage: i.usage }
      })
    },
    {
      name: 'MANUAL_TABLE',
      value: data['manual_table_indexes'].total_size,
      children: data['manual_table_indexes'].indexes.filter((k) => {
        return k.data_size > 0
      }).map((i) => {
        return { name: i.id, value: i.data_size, usage: i.usage }
      })
    }
  ]
}

export function formatFlowerJson (data) {
  let flowers = []
  let rootLevel = 0
  let maxLevel = 0

  data.forEach(roots => {
    // 获取树的最大level
    Object.values(roots.nodes).forEach(node => {
      node.level > maxLevel && (maxLevel = node.level)
    })
    // 把根节点push进flowers数组
    roots.roots.forEach(root => {
      root = getFlowerData(root, roots.nodes, maxLevel)
      flowers.push(root)
    })

    if (maxLevel > rootLevel) {
      rootLevel = maxLevel
    }
  })

  return [{
    name: 'root',
    id: 'root',
    size: (rootLevel + 1) ** 2 * 200 + 2500,
    children: flowers
  }]
}

export function getCuboidCounts (data) {
  let count = 0
  data.forEach(item => {
    count += Object.keys(item.nodes).length
  })
  return count
}

export function getStatusCuboidCounts (data, status) {
  let count = 0
  data.forEach(item => {
    const nodes = Object.values(item.nodes)
    for (const node of nodes) {
      if (node.cuboid.status === status) {
        count++
      }
    }
  })
  return count
}

function getFlowerData (cuboid, nodes = {}, maxLevel) {
  const isRoot = !~cuboid.parent
  if (isRoot) {
    cuboid.name = cuboid.cuboid.id
    cuboid.size = cuboid.cuboid.storage_size || 1
    cuboid.maxLevel = maxLevel
    cuboid.background = cuboid.cuboid.status
  }
  cuboid.children = cuboid.children.map((childId, index) => {
    const child = JSON.parse(JSON.stringify(nodes[childId]))
    child.name = child.cuboid.id
    child.size = child.cuboid.storage_size || 1
    child.background = child.cuboid.status
    child.maxLevel = maxLevel
    if (child.children && child.children.length) {
      getFlowerData(child, nodes, maxLevel)
    }
    return child
  })

  return cuboid
}
