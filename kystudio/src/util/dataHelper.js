/**
 *
 * @param {Object} options
 * @param {Array} [options.datas] 表格全量数据
 * @param {Number} [options.pageOffset] 表格分页页码
 * @param {Number} [options.pageSize] 表格分页尺寸
 * @param {Array} [options.filters] 筛选条件
 * 筛选条件 "或"关系：[条件1, 条件2]
 * 筛选条件 "与"关系：条件1 = { 条件3: 条件3, 条件4: 条件4 }
 */
export function getPaginationTable (options) {
  const { datas = [], pageOffset = 0, pageSize = 10, filters = [] } = options
  // 过滤符合筛选条件的每一行数据
  const filteredDatas = datas.filter(data => {
    let isMatchedData = false

    if (filters.length === 0) {
      isMatchedData = true
    }

    for (const filter of filters) {
      let isFilterMatch = true
      for (const [key, filterString] of Object.entries(filter)) {
        const dataValue = String(data[key]).toLowerCase()
        const filterValue = String(filterString).toLowerCase()

        if (!dataValue.includes(filterValue)) {
          isFilterMatch = false
          break
        }
      }
      isMatchedData = isMatchedData || isFilterMatch
    }

    return isMatchedData
  })
  // 分页
  const data = filteredDatas.slice(pageOffset * pageSize, pageOffset * pageSize + pageSize)
  const totalCount = filteredDatas.length

  if (!data.length && pageOffset > 0) {
    return getPaginationTable({ ...options, pageOffset: pageOffset - 1 })
  }

  return { data, pageOffset, pageSize, totalCount }
}
