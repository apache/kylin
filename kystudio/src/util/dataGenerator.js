import { sampleGuid } from './index'
/**
 * 找出lookup表的主键表和外键表
 * @param {Object} join 模型join关系
 * @param {String} alias lookup表别名
 */
function getPrimaryAndForeign (join, alias) {
  const [firstPKeys, ...pKeys] = join.primary_key || []
  const [firstFKeys, ...fKeys] = join.foreign_key || []

  const pTable = firstPKeys.split('.')[0]
  const fTable = firstFKeys.split('.')[0]
  // 模型数据结构不严谨，防错用
  if (alias !== pTable) {
    throw new Error(`Table [${alias}] has multiple joins.`)
  }
  // 模型数据结构不严谨，防错用
  for (const pKey of pKeys) {
    if (pKey.split('.')[0] !== pTable) {
      throw new Error(`Table [${alias}] has multiple joins.`)
    }
  }
  // 模型数据结构不严谨，防错用
  for (const fKey of fKeys) {
    if (fKey.split('.')[0] !== fTable) {
      throw new Error(`Table [${alias}] has multiple joins.`)
    }
  }
  return { primary: pTable, foreign: fTable }
}

/**
 * 获取并拼出模型所用到的table数据
 * @param {Object} modelData 原始模型object
 */
function getTablesData (modelData) {
  let tablesData = []

  try {
    // lookups和join_tables任选其一，组合成维度表集合
    const lookupsInfo = modelData.lookups || modelData.join_tables || []
    const factAlias = modelData.fact_table.split('.')[1]
    const factInfo = { table: modelData.fact_table, alias: factAlias, kind: 'FACT' }
    const tableColumnDataMap = modelData.simplified_tables.reduce((map, tableData) => ({
      ...map, [tableData.table]: tableData
    }), {})

    // 所有表集合 = 事实表 + 维度表
    const allTablesInfo = [factInfo, ...lookupsInfo]
    // 将simplified_tables和table_info组合，成为table data列表
    tablesData = allTablesInfo.map(tableInfo => ({
      guid: sampleGuid(),
      ...tableInfo,
      ...tableColumnDataMap[tableInfo.table]
    }))
  } catch (e) {
    console.warn(e)
  }

  return tablesData
}

function getJointsData (tablesData) {
  let jointsData = []

  for (const tableData of tablesData) {
    try {
      const { join, alias } = tableData
      if (tableData.kind === 'LOOKUP') {
        const { primary, foreign } = getPrimaryAndForeign(join, alias)
        const jointData = { guid: sampleGuid(), ...join, primary, foreign }
        jointsData = [...jointsData, jointData]
      }
    } catch (e) {
      console.warn(e)
    }
  }
  return jointsData
}

function formatJoinsData (jointData) {
  let joinsData = []

  try {
    const { primary_key: pKeys, foreign_key: fKeys } = jointData
    joinsData = pKeys.map((pKey, index) => ({
      guid: sampleGuid(),
      primaryKey: pKey,
      foreignKey: fKeys[index]
    }))
    // 模型数据结构不严谨，防错用
    if (pKeys.length !== fKeys.length) {
      throw new Error('The length of primary keys isn\'t the same as foreign keys.')
    }
  } catch (e) {
    console.warn(e)
  }

  return joinsData
}

export function getColumnNameMap (tablesData) {
  const columnNameMap = {}

  for (const tableData of tablesData) {
    const tableAlias = tableData.alias
    for (const column of tableData.columns) {
      columnNameMap[`${tableAlias}.${column.name}`] = column
    }
  }

  return columnNameMap
}

export function getDimensionsData (modelData, tablesData) {
  const columnNameMap = getColumnNameMap(tablesData)
  return modelData.simplified_dimensions.map(dimensionData => ({
    guid: sampleGuid(),
    dataType: columnNameMap[dimensionData.column].datatype,
    ...dimensionData
  }))
}

export function getMeasuresData (modelData) {
  return modelData.simplified_measures.map(measureData => ({
    guid: sampleGuid(),
    ...measureData
  }))
}

export function generateModel (modelData) {
  const tablesData = getTablesData(modelData)
  const jointsData = getJointsData(tablesData)
  const dimensionsData = getDimensionsData(modelData, tablesData)
  const measuresData = getMeasuresData(modelData)
  return {
    guid: modelData.uuid,
    name: modelData.name,
    alias: modelData.alias,
    tables: tablesData.map(tableData => ({
      guid: tableData.guid,
      name: tableData.table,
      alias: tableData.alias,
      type: tableData.kind
    })),
    dimensions: dimensionsData.map(dimensionData => ({
      guid: dimensionData.guid,
      name: dimensionData.name,
      table: dimensionData.column.split('.')[0],
      column: dimensionData.column.split('.')[1],
      dataType: dimensionData.dataType
    })),
    measures: measuresData.map(measureData => ({
      guid: measureData.guid,
      id: measureData.id,
      name: measureData.name,
      expression: measureData.expression,
      returnType: measureData.return_type,
      parameters: measureData.parameter_value,
      comment: measureData.comment
    })),
    joints: jointsData.map(jointData => ({
      guid: jointData.guid,
      primary: jointData.primary,
      foreign: jointData.foreign,
      type: jointData.type,
      joins: formatJoinsData(jointData)
    }))
  }
}
