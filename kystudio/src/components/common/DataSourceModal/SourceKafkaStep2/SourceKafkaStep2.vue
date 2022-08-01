<template>
  <div class="kafka-table-setting ksd-mtb-16 ksd-mrl-24">
    <el-form :model='kafkaMeta' :rules='rules' label-position="top" ref="kafkaForm">
      <el-form-item :label="$t('tableName')" required>
        <el-row :gutter="10">
          <el-col :span="12">
            <el-form-item prop="database">
              <el-select filterable allow-create v-model="kafkaMeta.database" @change="storeKafkaMeta" :placeholder="$t('inputDatabase')" size="medium" style="width:100%">
                <el-option v-for="(item, index) in databaseList" :key="index"
                :label="item"
                :value="item">
                </el-option>
              </el-select>
            </el-form-item>
          </el-col>
          <el-col :span="12">
            <el-form-item prop="name">
              <el-input size="medium" :placeholder="$t('inputTable')" @change="storeKafkaMeta" v-model="kafkaMeta.name"></el-input>
            </el-form-item>
          </el-col>
        </el-row>
      </el-form-item>
      <div class="ksd-mb-24">
        <span class="ksd-title-label-mini">{{$t('attachHiveToggle')}}</span>
        <el-tooltip effect="dark" :content="$t('attachHiveTips')" placement="top">
            <i class="el-ksd-icon-more_info_22 ksd-fs-22"></i>
          </el-tooltip>
        <el-switch
          v-model="isShowHiveTree"
          @change="storeKafkaMeta"
          size="small">
        </el-switch>
      </div>
      <el-form-item :label="$t('attachedHiveTable')" v-if="isShowHiveTree">
        <el-input size="medium" class="ksd-mb-8" prefix-icon="el-icon-search" v-global-key-event.enter.debounce="handleFilter" @clear="handleClear()" :placeholder="$t('filterTableName')" v-model="filterText"></el-input>
        <input type="hidden" name="batch_table_identity" v-model="kafkaMeta.batch_table_identity"/>
        <div class="hive-table-tree" v-loading="isLoadingTreeData">
          <TreeList
            :tree-key="treeKey"
            ref="treeList"
            :data="databaseArray"
            :placeholder="$t('filterTableName')"
            :default-expanded-keys="defaultExpandedKeys"
            :is-show-filter="false"
            :is-expand-on-click-node="false"
            :is-resizable="false"
            :on-filter="handleFilter"
            @click="handleClick"
            @load-more="handleLoadMore">
            <template>
              <kylin-nodata :content="emptyText" v-if="databaseArray.length <= 0 || tableArray.length <= 0" size="small"></kylin-nodata>
            </template>
          </TreeList>
        </div>
      </el-form-item>
      <div class="colums-table-block ksd-mtb-16">
        <div class="clearfix">
          <div class="ksd-title-label-small ksd-fleft">{{$t('columns')}}</div>
          <div  class="ksd-mb-10 ksd-fright">
            <el-input :placeholder="$t('inputColName')" size="medium" :prefix-icon="searchLoading? 'el-icon-loading':'el-icon-search'" @keypress.native.enter="handleFilterColumns" v-model="serarchColName" class="show-search-btn" >
            </el-input>
          </div>
        </div>
        <el-table
          :data='filterColumns'
          border
          height="200px"
          style='width: 100%' class="formTable">
          <!-- <el-table-column
          label=""
          align="center"
          width="55">
            <template slot-scope="scope">
              <el-checkbox v-model="scope.row.checked" @change="reNameCol(scope.row)" :disabled="scope.row.fromSource=='N'" true-label="Y" false-label="N"></el-checkbox>
            </template>
          </el-table-column> -->
          <el-table-column
          :label="$t('column')"
          show-overflow-tooltip
          property="name">
          </el-table-column>
          <!-- <el-table-column
          :label="$t('column')"
          show-overflow-tooltip
          property="name">
            <template slot-scope="scope">
              <el-input size="small" :class="{'is-colReName': scope.row.isReName}" v-model="scope.row.name" @input="reNameCol(scope.row)" :disabled="scope.row.fromSource=='N'"></el-input>
            </template>
          </el-table-column> -->
          <el-table-column
          :label="$t('columnType')">
            <template slot-scope="scope">
              <el-select v-model="scope.row.datatype" size="small" @change="handleChangeDatatype(scope.row)" :disabled="scope.row.fromSource=='N' || isShowHiveTree">
                <el-option
                  v-for="(item, index) in dataTypes"
                  :key="index"
                  :label="item"
                  :value="item"
                  >
                </el-option>
              </el-select>
            </template>
          </el-table-column>
          <el-table-column
            :label="$t('sampleValue')"
            show-overflow-tooltip
            property="value">
          </el-table-column>
          <el-table-column
          property="comment"
          :label="$t('comment')">
            <template slot-scope="scope">
              <el-tag size="mini" v-if="scope.row.fromSource=='N'" >{{$t('derivedTimeDimension')}}</el-tag>
              <el-input size="small" v-else v-model="scope.row.comment"></el-input>
            </template>
          </el-table-column>
        </el-table>
        <div class="column-rename-msg" v-if="isShowColNameError">
          {{colNameError}}
        </div>
      </div>
    </el-form>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapGetters, mapActions } from 'vuex'
import { Component } from 'vue-property-decorator'
import { NamedRegex, pageSizeMapping } from 'config'
import locales from './locales'
import TreeList from '../../TreeList'
import { handleError, handleSuccess, handleSuccessAsync, sampleGuid, indexOfObjWithSomeKey } from 'util'
import { getDatasourceObj, getTableObj, getDatabaseTablesObj } from 'util/datasourceDataHandler'
import { objectClone } from '../../../../util'
@Component({
  props: {
    columnData: {
      default: () => null
    },
    convertData: {
      default: () => null
    }
  },
  components: {
    TreeList
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  methods: {
    ...mapActions({
      fetchDatabase: 'LOAD_HIVEBASIC_DATABASE',
      fetchDBandTables: 'FETCH_DB_AND_TABLES',
      fetchTables: 'FETCH_TABLES'
    })
  },
  locales
})
export default class SourceKafkaStep2 extends Vue {
  reNameList = []
  isShowColNameError = false
  parserErrorMsg = ''
  isParserErrorShow = false
  convertLoading = false
  columnList = []
  filterColumns = []
  rules = {
    name: [
      { required: true, message: this.$t('pleaseInputTableName'), trigger: 'blur' },
      {validator: this.checkName, trigger: 'blur'}
    ],
    timestampField: [
      { required: true, message: this.$t('kylinLang.common.pleaseSelect'), trigger: 'change' }
    ],
    tsParser: [
      { required: true, message: this.$t('kylinLang.common.pleaseSelect'), trigger: 'change' }
    ],
    tsPattern: [
      { required: true, message: this.$t('kylinLang.common.pleaseSelect'), trigger: 'change' }
    ],
    tsTimezone: [
      { required: true, message: this.$t('kylinLang.common.pleaseInput'), trigger: 'change' }
    ]
  }
  streamingCfg = {
    columnOptions: [],
    timestampField: ''
  }
  streamingAutoGenerateMeasure = [
    {name: 'year_start', type: 'date'},
    {name: 'quarter_start', type: 'date'},
    {name: 'month_start', type: 'date'},
    {name: 'week_start', type: 'date'},
    {name: 'day_start', type: 'date'},
    {name: 'hour_start', type: 'timestamp'},
    {name: 'minute_start', type: 'timestamp'}
  ]
  dataTypes = ['tinyint', 'smallint', 'int', 'bigint', 'float', 'double', 'decimal', 'timestamp', 'date', 'string', 'varchar(256)', 'char', 'boolean', 'binary']
  kafkaMeta = {
    database: '',
    name: '',
    columns: [],
    batch_table_identity: '',
    has_shadow_table: false,
    starting_offsets: 'latest',
    source_type: 1,
    subscribe: '',
    parser_name: 'org.apache.kylin.parser.TimedJsonStreamParser',
    kafka_bootstrap_servers: ''
  }
  databaseList = []
  serarchColName = ''
  verified = false
  isVerifyErrorShow = false
  verifyErrorMsg = ''
  verifyData = null
  verifyTable = []
  searchLoading = false
  filterText = ''
  isShowHiveTree = true
  treeKey = 'hivetree' + Number(new Date())
  isLoadingTreeData = false
  hideFactIcon = true
  isShowCancelSelected = true
  datasources = []
  defaultExpandedKeys = []
  ignoreNodeTypes = ['column']
  batchTableColumns = []
  get emptyText () {
    return this.filterText ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
  }
  checkName (rule, value, callback) {
    if (!NamedRegex.test(value)) {
      callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
    } else {
      callback()
    }
  }
  handleChangeDatatype (row) {
    const index = indexOfObjWithSomeKey(this.kafkaMeta.columns, 'guid', row.guid)
    if (index !== -1) {
      this.$set(this.kafkaMeta.columns[index], 'datatype', row.datatype)
      this.storeKafkaMeta()
    }
  }
  storeKafkaMeta () {
    this.kafkaMeta.name = this.kafkaMeta.name.toUpperCase()
    const kafkaMetaObj = objectClone(this.kafkaMeta)
    delete kafkaMetaObj.columns
    this.$emit('input', { kafkaMeta: {
      isShowHiveTree: this.isShowHiveTree,
      batchTableColumns: this.batchTableColumns,
      kafka_config: kafkaMetaObj,
      table_desc: { name: this.kafkaMeta.name, columns: this.kafkaMeta.columns, source_type: 1, database: this.kafkaMeta.database },
      project: this.currentSelectedProject
    } })
  }
  resetVerify () {
    this.verified = false
    this.isVerifyErrorShow = false
    this.verifyErrorMsg = ''
    this.verifyData = null
    this.verifyTable = []
    this.$emit('isVerifyKafka', this.verified)
  }
  convertJson (columnData) {
    this.reNameList = []
    function checkValType (val, key) {
      var defaultType
      if (typeof val === 'number') {
        if (/id/i.test(key) && val.toString().indexOf('.') === -1) {
          defaultType = 'int'
        } else if (val <= 2147483647) {
          if (val.toString().indexOf('.') !== -1) {
            defaultType = 'decimal'
          } else {
            defaultType = 'int'
          }
        } else {
          defaultType = 'timestamp'
        }
      } else if (typeof val === 'string') {
        if (!isNaN((new Date(val)).getFullYear()) && typeof ((new Date(val)).getFullYear()) === 'number') {
          defaultType = 'date'
        } else {
          defaultType = 'varchar(256)'
        }
      } else if (Object.prototype.toString.call(val) === '[object Array]') {
        defaultType = 'varchar(256)'
      } else if (typeof val === 'boolean') {
        defaultType = 'boolean'
      }
      return defaultType
    }

    function createNewObj (key, val) {
      var obj = {}
      obj.attribute = key
      obj.name = key
      obj.datatype = checkValType(val, key)
      obj.value = val
      obj.fromSource = 'Y'
      obj.checked = 'Y'
      obj.comment = ''
      obj.case_sensitive_name = null
      obj.guid = sampleGuid()
      if (Object.prototype.toString.call(val) === '[object Array]') {
        obj.checked = 'N'
      }
      return obj
    }
    let columnList = []
    for (var key in columnData) {
      columnList.push(createNewObj(key, columnData[key]))
    }
    return columnList
  }
  handleFilterColumns () {
    this.$nextTick(() => {
      this.filterColumns = this.columnList.filter((col) => {
        return col.name.indexOf(this.serarchColName.toLocaleUpperCase()) !== -1
      })
    })
  }
  async loadKakaDatabase () {
    try {
      const projectName = this.currentSelectedProject
      const res = await this.fetchDatabase({ projectName })
      const data = await handleSuccessAsync(res)
      this.databaseList = data
      this.kafkaMeta.database = data[0]
    } catch (e) {
      handleError(e)
    }
  }
  loadKafkaClusters () {
    this.loadClusters({project: this.project}).then((res) => {
      handleSuccess(res, (data) => {
        this.kafkaMeta.clusters = data
        this.$nextTick(() => {
          this.$refs.clustercompoment.initData()
        })
      })
    }, (res) => {
      handleError(res)
    })
  }
  get databaseArray () {
    const allData = this.datasources.reduce((databases, datasource) => [...databases, ...datasource.children], [])
    return allData.filter(data => !['isMore', 'isLoading'].includes(data.type))
  }
  get tableArray () {
    const allData = this.databaseArray.reduce((tables, database) => [...tables, ...database.children], [])
    return allData.filter(data => !['isMore', 'isLoading'].includes(data.type))
  }
  get columnArray () {
    return this.ignoreNodeTypes.indexOf('column') >= 0 ? [] : this.tableArray.reduce((columns, table) => [...columns, ...table.children], [])
  }
  async loadTreeData (filterText) {
    this.treeKey = 'hiveTree_' + filterText + Number(new Date())
    this.isLoadingTreeData = true
    this.datasources = []
    const sourceType = 9 // 这里只加载Hive数据源数据
    const params = {
      project_name: this.currentSelectedProject,
      source_type: sourceType,
      page_offset: 0,
      page_size: pageSizeMapping.TABLE_TREE,
      table: filterText || ''
    }
    const response = await this.fetchDBandTables(params)
    const results = await handleSuccessAsync(response)
    if (results.databases.length) {
      const datasource = getDatasourceObj(this, sourceType)
      // 先处理 db 一层的render，以及初值的赋值
      datasource.children = results.databases.map(resultDatabse => getDatabaseTablesObj(this, datasource, resultDatabse))
      // 然后处理 table 一级的render render 后变更页码，并存入store
      datasource.children.forEach((database, index) => {
        database.children = database.originTables.map(resultTable => getTableObj(this, database, resultTable, true, true))
        this.addPagination(database)
      })
      this.datasources.push(datasource)
      this.defaultExpandedKeys.push(datasource.id)
      this.defaultExpandedKeys.push(datasource.children[0].id)
    }
    this.isLoadingTreeData = false
  }
  addPagination (data) {
    data.pagination.page_offset++
  }
  clearPagination (data) {
    data.pagination.page_offset = 0
  }
  async handleLoadMore (data, node) {
    let dbName = (data.parent.label).toLocaleLowerCase()
    const { id: databaseId } = data.parent
    // 加载更多时，要将查询的关键字解析处理
    let tableName = ''
    // 如果完全匹配 db，或者是搜索的关键字包含在 dbName 中，这时搜索table的关键字应该是空
    if (dbName.indexOf(this.filterText.toLocaleLowerCase()) > -1) {
      tableName = ''
    } else { // 只有没有完全匹配db时，才会将关键字传
      let idx = this.filterText.indexOf('.')
      tableName = idx === -1 ? this.filterText : this.filterText.substring(idx + 1, this.filterText.length)
    }
    await this.loadTables({ databaseId, tableName })
  }
  async loadTables (params) {
    try {
      const { tableName = null, databaseId = null, isReset = false } = params || {}
      const currentDatabases = this.databaseArray.filter(database => {
        return database.id === databaseId || !databaseId
      })
      const responses = await Promise.all(currentDatabases.map((database) => {
        const { projectName, label: databaseName, pagination, datasource } = database
        isReset ? this.clearPagination(database) : null
        return this.fetchTables({ projectName, databaseName, tableName, isExt: true, ...pagination, sourceType: datasource })
      }))
      const results = await handleSuccessAsync(responses)
      currentDatabases.forEach((database, index) => {
        const { size, tables: resultTables } = results[index]
        const tables = resultTables.map(resultTable => getTableObj(this, database, resultTable, this.ignoreNodeTypes.indexOf('column') >= 0, true))
        if (this.ignoreNodeTypes.indexOf('table') < 0) {
          database.children = !isReset ? [...database.children, ...tables] : tables
          database.isMore = size && size > this.getChildrenCount(database)
          database.isHidden = !this.getChildrenCount(database)
        }
        this.addPagination(database)
      })
    } catch (e) {
      handleError(e)
    }
  }
  getChildrenCount (data) {
    return data.children.filter(data => !['isMore', 'isLoading'].includes(data.type)).length
  }
  cancelSelected (data, node, event) {
    if (data && node && event) {
      event && event.stopPropagation()
      event && event.preventDefault()
      data.isSelected = false
      this.kafkaMeta.batch_table_identity = ''
      this.kafkaMeta.has_shadow_table = false
      this.batchTableColumns = []
      this.storeKafkaMeta()
    } else {
      this.databaseArray.forEach(d => {
        d.children.forEach(t => {
          t.isSelected = false
        })
      })
    }
  }
  handleClick (node) {
    if (node.database && node.label) {
      this.cancelSelected()
      node.isSelected = true
      this.kafkaMeta.batch_table_identity = node.database + '.' + node.label
      this.kafkaMeta.has_shadow_table = true
      this.batchTableColumns = node.__data.columns
      this.storeKafkaMeta()
    }
  }
  handleFilter () {
    this.loadTreeData(this.filterText)
  }
  handleClear () {
    this.loadTreeData()
  }
  created () {
    this.loadKakaDatabase()
    // this.loadKafkaClusters()
    this.loadTreeData()
    this.columnList = this.convertJson(this.columnData)
    this.handleFilterColumns()
    this.kafkaMeta.columns = this.columnList.map((col, index) => {
      col.name = col.name.toLocaleUpperCase()
      col.id = index + 1
      return col
    })
    this.kafkaMeta.subscribe = this.convertData && this.convertData.name
    this.kafkaMeta.kafka_bootstrap_servers = this.convertData && this.convertData.kafka.kafka_config.kafka_bootstrap_servers
    this.kafkaMeta.project = this.currentSelectedProject
    this.$emit('input', { kafkaMeta: this.kafkaMeta })
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';
.kafka-table-setting {
  .colums-table-block {
    padding: 10px;
    background-color: @breadcrumbs-bg-color;
    .show-search-btn {
      width: 220px;
    }
    .column-rename-msg {
      color: @error-color-1;
      font-size: 12px;
    }
  }
  .hive-table-tree {
    border: 1px solid @ke-border-divider-color;
    height: 200px;
    max-height: 400px;
    overflow: hidden;
    overflow-y: auto;
    .tree-list {
      height: 100%;
    }
    .tree-item {
      position: relative;
      width: 100%;
      height: 30px;
      line-height: 30px;
      &.selected {
        .frontground {
          position: relative;
          z-index: 1;
        }
        .background {
          background: @base-color-9;
          position: absolute;
          top: 0;
          right: 0;
          bottom: 0;
          left: calc(-24px - 36px);
        }
        .right {
          float: right;
        }
      }
    }
  }
}
</style>
