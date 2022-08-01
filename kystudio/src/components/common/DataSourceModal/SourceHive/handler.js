import { pageSizeMapping } from '../../../../config'

export function getDatabaseTablesTree (databases) {
  return databases.map(database => ({
    id: database.dbname,
    label: database.dbname,
    children: [],
    originTables: database.tables || [],
    size: database.size,
    type: 'database',
    isMore: database.size && database.size > database.tables.length,
    isLoading: false,
    isSelected: false,
    pagination: {
      page_offset: 0,
      pageSize: pageSizeMapping.TABLE_TREE
    },
    render: (h, { node, data, store }) => {
      const { label } = data
      return (
        <div class="database">
          <div class="label el-tree-tooltip-box">
            {data.isSelected ? (
              <span class="el-icon-ksd-good_health"></span>
            ) : null}
            <i class="el-icon-ksd-data_source"></i> {label}
          </div>
          <div class="select-all" onClick={event => this.handleSelectDatabase(event, data)}>
            {data.isSelected ? this.$t('cleanAll') : this.$t('selectAll')}
          </div>
        </div>
      )
    }
  }))
}

export function getDatabaseTree (databases) {
  return databases.map(databaseName => ({
    id: databaseName,
    label: databaseName,
    children: [],
    type: 'database',
    isMore: false,
    isLoading: true,
    isSelected: false,
    pagination: {
      page_offset: 0,
      pageSize: pageSizeMapping.TABLE_TREE
    },
    render: (h, { node, data, store }) => {
      const { label } = data
      return (
        <div class="database">
          <div class="label">
            {data.isSelected ? (
              <span class="el-icon-ksd-good_health"></span>
            ) : null}
            <i class="el-icon-ksd-data_source"></i> {label}
          </div>
          <div class="select-all" onClick={event => this.handleSelectDatabase(event, data)}>
            {data.isSelected ? this.$t('cleanAll') : this.$t('selectAll')}
          </div>
        </div>
      )
    }
  }))
}

export function getTableTree (database, res, isTableReset, selectTablesNames) {
  const newTables = res.tables.map(table => ({
    id: `${database.id}.${table.table_name}`,
    label: table.table_name,
    type: 'table',
    database: database.id,
    isSelected: table.loaded || database.isSelected || table.table_name && selectTablesNames && selectTablesNames.includes(`${database.id}.${table.table_name}`),
    clickable: !table.loaded && !database.isSelected && !table.existed,
    isLoaded: table.loaded,
    isExistedName: table.existed,
    render: (h, { node, data, store }) => {
      const isChecked = !data.isLoaded && data.isSelected && !table.existed
      const isLoaded = data.isLoaded
      const isExistedName = data.isExistedName
      const isAllTableSelected = database.children.filter(item => item.type !== 'isMore').every(item => item.isLoaded)
      const tableClassNames = [
        'table',
        ...(database.isSelected && !isAllTableSelected ? ['parent-selected'] : []),
        ...(!data.clickable ? ['disabled'] : []),
        ...(isLoaded ? ['synced'] : []),
        ...(isExistedName ? ['existedName'] : [])
      ]
      const currentId = `table-load-${data.id}`
      setTimeout(() => {
        const currentEl = document.getElementById(currentId)
        const parentEl = currentEl && currentEl.parentNode.parentNode.parentNode
        if (parentEl) {
          parentEl.style.cssText = `background-color: ${!data.clickable ? 'transparent' : null}; cursor: ${!data.clickable ? 'not-allowed' : null};`
        }
      }, 100)
      const itemClassName = isLoaded || isExistedName ? 'is-synced' : ''
      return (
        <div class={itemClassName}>
          <div class={tableClassNames} id={currentId}>
            { isChecked ? (
              <span class="el-icon-ksd-good_health"></span>
            ) : null }
            <span>{data.label}</span>
          </div>
          { isLoaded ? (
            <span class="label-synced">{this.$t('synced')}</span>
          ) : null }
          { isExistedName ? (
            <span class="label-synced">{this.$t('existedName')}</span>
          ) : null }
        </div>
      )
    }
  }))
  database.children = isTableReset
    ? newTables
    : database.children.concat(newTables)
  database.isMore = res.size > getChildrenCount(database)
  database.isLoading = false
}

function getChildrenCount (data) {
  return data.children.filter(data => !['isMore', 'isLoading'].includes(data.type)).length
}
