import { isDatePartitionType } from '.'

export function getFormattedTable (originData = {}) {
  const partitionColumn = originData.partitioned_column && originData.partitioned_column.split('.')[1] || ''
  const dateTypeColumns = originData.columns.filter(column => isDatePartitionType(column.datatype))
  const storageType = getStorageType(originData)
  const [ startTime, endTime ] = _getSegmentRange(originData)
  return {
    uuid: originData.uuid,
    name: originData.name,
    database: originData.database,
    fullName: `${originData.database}.${originData.name}`,
    updateAt: originData.last_modified,
    datasource: originData.source_type,
    cardinality: originData.cardinality,
    last_build_job_id: originData.last_build_job_id,
    partitionColumn,
    format: originData.partitioned_column_format || 'yyyy-MM-dd',
    storageType,
    storageSize: ~originData.storage_size ? originData.storage_size : null,
    totalRecords: ~originData.total_records ? originData.total_records : null,
    columns: originData.columns,
    dateTypeColumns,
    startTime,
    endTime,
    create_time: originData.create_time,
    sampling_rows: originData.sampling_rows,
    kafka_bootstrap_servers: originData.kafka_bootstrap_servers,
    subscribe: originData.subscribe,
    batch_table_identity: originData.batch_table_identity,
    __data: originData
  }
}

function getStorageType (originData = {}) {
  const isLookup = originData.lookup
  const isFact = originData.root_fact
  const hasPartition = originData.partitioned_column

  if (hasPartition) {
    return 'incremental'
  } else if (isFact) {
    return 'full'
  } else if (isLookup) {
    return 'snapshot'
  } else {
    return null
  }
}

function _getSegmentRange (originData) {
  const segmentRange = originData.segment_range
  if (segmentRange) {
    const startTime = segmentRange.date_range_start
    const endTime = segmentRange.date_range_end
    return [ startTime, endTime ]
  } else {
    return []
  }
}

/**
 * 获取所有Segment数据范围
 * @param {*} table
 */
export function getAllSegmentsRange (table) {
  // const segmentKeyValuePairs = Object.entries(table.segment_ranges)
  // let maxTime = -Infinity
  // let minTime = Infinity
  // for (const [key] of segmentKeyValuePairs) {
  //   const [ startTime, endTime ] = key.replace(/^TimePartitionedSegmentRange\[|\)$/g, '').split(',')
  //   startTime < minTime ? (minTime = +startTime) : null
  //   endTime > maxTime ? (maxTime = +endTime) : null
  // }
  // return segmentKeyValuePairs.length ? [ minTime, maxTime ] : []
  return []
}

export function getAllSegments (segmentData) {
  // return Object.entries(segmentData).map(([key, value]) => {
  //   const [ startTime, endTime ] = key.replace(/^TimePartitionedSegmentRange\[|\)$/g, '').split(',')
  //   return { startTime, endTime, status: value }
  // }).sort((segmentA, segmentB) => {
  //   return segmentA.startTime < segmentB.startTime ? -1 : 1
  // })
  return []
}
