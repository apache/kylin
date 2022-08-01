import { sourceTypes, sourceNameMapping, pageSizeMapping, columnTypeIcon } from '../../../config'
import { transToServerGmtTime } from '../../../util'
export const render = {
  datasource: {
    render (h, { node, data, store }) {
      const { label } = data
      return (
        <div class="datasource font-medium">
          <span>{label}</span>
        </div>
      )
    }
  },
  database: {
    render (h, { node, data, store }) {
      const { label, isDefaultDB } = data
      return (
        <div class="database">
          <div class="left">
            <i class="tree-icon el-icon-ksd-data_source"></i>
          </div>
          <span title={label}>{label}</span><span class="defaultIcon">{isDefaultDB ? ' (Default)' : ''}</span>
        </div>
      )
    }
  },
  table: {
    render (h, { node, data, store }) {
      const { label, tags, dateRange, isTopSet } = data
      const dataRangeTitle = this.$t('dataRange')
      const nodeClass = {
        class: [
          'frontground',
          'table',
          'el-tree-tooltip-box',
          ...(dateRange ? ['has-range'] : [])
        ]
      }

      return (
        <div>
          <div {...nodeClass}>
            <span title={label}>{label}</span>
            <div class="right">
              <span class="tree-icon" slot="reference">
                <el-tooltip effect="dark" enterable={false} content={isTopSet ? this.$t('cancelTopSet') : this.$t('topSet')} placement="top">
                  <i class="table-date-tip top" onClick={event => this.handleToggleTop(data, node, event)}
                    { ...{class: data.isTopSet ? ['el-icon-ksd-arrow_up_clean'] : ['el-icon-ksd-arrow_up']} }></i>
                </el-tooltip>
              </span>
              { dateRange ? (
                <el-popover
                  placement="right"
                  title={dataRangeTitle}
                  trigger="hover"
                  content={dateRange}>
                  <i class="tree-icon table-date-tip el-icon-ksd-data_range" slot="reference"></i>
                </el-popover>
              ) : null }
            </div>
            <div class="right fact-icon">
              {tags.map(tag => {
                switch (tag) {
                  case 'F':
                    if (data.isHideFactIcon) {
                      return <i class="tree-icon"></i>
                    } else {
                      return <el-tooltip effect="dark" enterable={false} content={this.$t('factTable')} placement="top"><i class="tree-icon el-icon-ksd-fact_table"></i></el-tooltip>
                    }
                  case 'L':
                    return <i class="tree-icon"></i>
                    // return <i class="tree-icon el-icon-ksd-lookup_table"></i>
                  case 'N':
                  default:
                    return <i class="tree-icon"></i>
                    // return <i class="tree-icon el-icon-ksd-sample"></i>
                }
              })}
            </div>
          </div>
          <div class="background"></div>
        </div>
      )
    }
  },
  column: {
    render (h, { node, data, store }) {
      const { label, tags, datatype, cardinality, min_value, max_value } = data
      return (
        <div class="column">
          <div class="left">
            {tags.map(tag => {
              switch (tag) {
                case 'FK':
                  return <i class="tree-icon column-tag el-icon-ksd-symbol_fk"></i>
                case 'PK':
                  return <i class="tree-icon column-tag el-icon-ksd-symbol_pk"></i>
              }
            })}
          </div>
          <el-tooltip placement="top" visible-arrow={false} open-delay={300}>
            <div slot="content">
              <span>{this.$t('kylinLang.model.columnName')}</span>: <span>{label === null ? 'NULL' : label}</span><br/>
              <span>{this.$t('kylinLang.dataSource.dataType')}</span>: <span>{datatype === null ? 'NULL' : datatype}</span><br/>
              <span>{this.$t('kylinLang.dataSource.cardinality')}</span>: <span>{cardinality === null ? 'NULL' : cardinality}</span><br/>
              <span>{this.$t('kylinLang.dataSource.minimal')}</span>: <span>{min_value === null ? 'NULL' : min_value}</span><br/>
              <span>{this.$t('kylinLang.dataSource.maximum')}</span>: <span>{max_value === null ? 'NULL' : max_value}</span>
            </div>
            <span><div></div>
              <i class={columnTypeIcon(datatype)}></i>
              <span class="column-name"> {label}</span>
            </span>
          </el-tooltip>
        </div>
      )
    }
  }
}

export function getDatasourceObj (that, sourceType) {
  const { override_kylin_properties } = that.currentProjectData
  const { projectName, customTreeTitle } = that
  const sourceName = sourceTypes[sourceType]
  let sourceNameStr = 'kylin.source.jdbc.source.enable' in override_kylin_properties && override_kylin_properties['kylin.source.jdbc.source.enable'] === 'true' && override_kylin_properties['kylin.source.default'] === '8'
    ? override_kylin_properties['kylin.source.jdbc.source.name'] || sourceNameMapping[sourceName]
    : sourceNameMapping[sourceName]
  return {
    id: sourceType,
    label: customTreeTitle !== '' ? `${that.$t(customTreeTitle)}` : `${that.$t('source')}${sourceNameStr}`,
    render: render.datasource.render.bind(that),
    children: [],
    sourceType,
    projectName,
    type: 'datasource'
  }
}

export function getDatabaseObj (that, datasource, databaseItem) {
  const { projectName } = datasource
  return {
    id: `${datasource.id}.${databaseItem}`,
    label: databaseItem,
    isDefaultDB: that.$store.state.project.projectDefaultDB === databaseItem,
    render: render.database.render.bind(that),
    children: [],
    type: 'database',
    datasource: datasource.id,
    isMore: false,
    isHidden: false,
    isLoading: true,
    projectName,
    parent: datasource,
    pagination: {
      page_offset: 0,
      pageSize: pageSizeMapping.TABLE_TREE
    }
  }
}

export function getDatabaseTablesObj (that, datasource, databaseItem) {
  const { projectName } = datasource
  return {
    id: `${datasource.id}.${databaseItem.dbname}`,
    label: databaseItem.dbname,
    isDefaultDB: that.$store.state.project.projectDefaultDB === databaseItem.dbname,
    render: render.database.render.bind(that),
    children: [],
    originTables: databaseItem.tables || [],
    type: 'database',
    datasource: datasource.id,
    isMore: databaseItem.size && databaseItem.size > databaseItem.tables.length,
    isHidden: !databaseItem.tables || !databaseItem.tables.length,
    isLoading: false,
    projectName,
    parent: datasource,
    pagination: {
      page_offset: 0,
      pageSize: pageSizeMapping.TABLE_TREE
    }
  }
}

export function getTableObj (that, database, table, ignoreColumn) {
  const { datasource, label: databaseName } = database
  const tags = [
    ...(table.root_fact ? ['F'] : []),
    ...(table.lookup ? ['L'] : []),
    ...(!table.root_fact && !table.lookup ? ['N'] : [])
  ]
  const dataRange = _getSegmentRange(table)
  const dateRangeStr = _getDateRangeStr(that, dataRange)
  const tableObj = {
    id: table.uuid,
    label: table.name,
    children: [],
    render: render.table.render.bind(that),
    tags,
    type: 'table',
    database: databaseName,
    datasource,
    isCentral: table.increment_loading,
    isTopSet: table.top,
    isHideFactIcon: that.hideFactIcon,
    dateRange: dateRangeStr,
    isSelected: false,
    parent: database,
    isMore: false,
    child_options: {
      page_offset: 1,
      page_size: 10
    },
    __data: table
  }
  if (!ignoreColumn) {
    let columnList = getColumnObjArray(that, tableObj).sort((itemA, itemB) => itemA.label > itemB.label ? 1 : -1)
    tableObj.children = columnList.slice(0, tableObj.child_options.page_size)
    tableObj.childContent = columnList
    tableObj.isMore = tableObj.childContent.length > tableObj.child_options.page_size
  } else {
    tableObj.children = null
  }
  return tableObj
}

function getColumnObjArray (that, tableObj) {
  const { foreignKeys, primaryKeys } = that
  const { label: tableName, database, datasource } = tableObj
  const { columns } = tableObj.__data

  return columns.map(column => {
    const columnFullName = `${datasource}.${database}.${tableName}.${column.name}`
    const tags = [
      ...(foreignKeys.includes(columnFullName) ? ['FK'] : []),
      ...(primaryKeys.includes(columnFullName) ? ['PK'] : [])
    ]
    return {
      id: `${tableObj.id}.${column.name}`,
      label: column.name,
      render: render.column.render.bind(that),
      tags,
      type: 'column',
      datatype: column.datatype,
      cardinality: column.cardinality,
      min_value: column.min_value,
      max_value: column.max_value
    }
  })
}

export function getWordsData (data) {
  return {
    meta: data.type,
    caption: data.label,
    value: data.label,
    id: data.id,
    scope: 1
  }
}

export function getTableDBWordsData (data) {
  return {
    meta: data.type,
    caption: `${data.database}.${data.label}`,
    value: `${data.database}.${data.label}`,
    id: data.id,
    scope: 1
  }
}

export function getFirstTableData (datasourceTree) {
  for (const datasource of datasourceTree) {
    for (const database of datasource.children) {
      if (database.children && database.children[0]) {
        return database.children && database.children[0]
      }
    }
  }
}

export function freshTreeOrder (that) {
  that.datasources.forEach(datasource => {
    // 先将默认库取出来，将默认库以外的按字母排序
    let tempArr = datasource.children.filter((db) => {
      return db.label !== that.$store.state.project.projectDefaultDB
    })
    let defaultDB = datasource.children.filter((db) => {
      return db.label === that.$store.state.project.projectDefaultDB
    })
    if (defaultDB.length) {
      tempArr.sort((itemA, itemB) => itemA.label > itemB.label ? 1 : -1)
      tempArr.unshift(defaultDB[0])
    } else {
      tempArr.sort((itemA, itemB) => itemA.label > itemB.label ? 1 : -1)
    }
    datasource.children = tempArr
    // datasource.children.sort((itemA, itemB) => itemA.label > itemB.label ? 1 : -1)
    datasource.children.forEach(database => {
      database.children.sort((itemA, itemB) => {
        if (itemA.isTopSet !== itemB.isTopSet) {
          return itemA.isTopSet && !itemB.isTopSet ? -1 : 1
        } else {
          if (itemA.isCentral !== itemB.isCentral) {
            return itemA.isCentral && !itemB.isCentral ? -1 : 1
          } else {
            return itemA.label < itemB.label ? -1 : 1
          }
        }
      })
    })
  })
  that.datasources = [...that.datasources]
}

function _getDateRangeStr (that, userRange) {
  const [ startTime, endTime ] = userRange
  if (startTime !== undefined && endTime !== undefined) {
    const startStr = transToServerGmtTime(startTime)
    const endStr = transToServerGmtTime(endTime)
    return `${startStr} ${that.$t('to')} ${endStr}`
  } else {
    return ''
  }
}

function _getSegmentRange (table) {
  const segmentRange = table.segment_range
  if (segmentRange) {
    const startTime = segmentRange.date_range_start
    const endTime = segmentRange.date_range_end
    return [ startTime, endTime ]
  } else {
    return []
  }
}
