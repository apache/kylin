export function _getPartitionInfo (project, table, partitionColumn, format) {
  return {
    project: project.name,
    table: `${table.database}.${table.name}`,
    column: partitionColumn,
    partition_column_format: format
  }
}

export function _getRefreshFullLoadInfo (project, table) {
  return {
    projectName: project.name,
    tableFullName: `${table.database}.${table.name}`,
    startTime: '0',
    endTime: '9223372036854775807'
  }
}

export function _getFullLoadInfo (project, table) {
  return {
    projectName: project.name,
    tableFullName: `${table.database}.${table.name}`,
    startTime: String(0),
    endTime: '9223372036854775807',
    affectedStart: '',
    affectedEnd: ''
  }
}
