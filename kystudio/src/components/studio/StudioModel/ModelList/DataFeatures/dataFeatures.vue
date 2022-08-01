<template>
  <div class="data_features-layout">
    <div class="data_features-database">
      <div class="data_features-database-list">
        <tree-list
          class="data_features-trees"
          ref="data_features-trees"
          :showOverflowTooltip="true"
          :data="dbList"
          :default-checked-keys="defaultCheckKeys"
          @click="handleNodeClick" isExpandAll
        >
        </tree-list>
      </div>
    </div>
    <div class="data_features-details">
      <el-tabs class="data_features-tabs" v-model="tabType" v-loading="loadingData" tab-position="top" @tab-click="handleTabClick">
        <el-tab-pane :label="$t('dataFeatures')" name="dataFeatures">
          <template v-if="tabType === 'dataFeatures'">
            <el-table
              v-scroll-shadow
              :data="statistics.list"
              class="data_features-tables"
              ref="dataFeatureTable"
              style="width: 100%">
              <el-table-column
                prop="name"
                :label="$t('columnName')"
                width="300"
                show-overflow-tooltip
              >
              </el-table-column>
              <el-table-column
                prop="cardinality"
                :label="$t('cardinality')"
                show-overflow-tooltip
              >
                <template slot-scope="scope">
                  <span v-if="scope.row.cardinality !== null">{{scope.row.cardinality}}</span>
                  <span v-else class="null-value"><i>NULL</i></span>
                </template>
              </el-table-column>
              <el-table-column
                prop="max_value"
                :label="$t('maxValue')"
                show-overflow-tooltip
              >
                <template slot-scope="scope">
                  <span v-if="scope.row.max_value !== null">{{scope.row.max_value}}</span>
                  <span v-else class="null-value"><i>NULL</i></span>
                </template>
              </el-table-column>
              <el-table-column
                prop="min_value"
                :label="$t('minValue')"
                show-overflow-tooltip
              >
                <template slot-scope="scope">
                  <span v-if="scope.row.min_value !== null">{{scope.row.min_value}}</span>
                  <span v-else class="null-value"><i>NULL</i></span>
                </template>
              </el-table-column>
              <el-table-column
                prop="null_count"
                :label="$t('zeroCount')"
                show-overflow-tooltip
              >
                <template slot-scope="scope">
                  <span v-if="scope.row.null_count !== null">{{scope.row.null_count}}</span>
                  <span v-else class="null-value"><i>NULL</i></span>
                </template>
              </el-table-column>
            </el-table>
            <el-pagination v-if="!loadingData" class="ksd-center ksd-mtb-10" ref="dataFeaturesPager" layout="total, sizes, prev, pager, next, jumper" :total="statistics.totalSize" :page-size="statistics.pageSize" :current-page="statistics.pageOffset + 1" @size-change="(val) => pageStatisticsChange('size', val)" @current-change="(val) => pageStatisticsChange('page', val)"></el-pagination>
          </template>
        </el-tab-pane>
        <el-tab-pane :label="$t('sample')" name="sample" v-if="!isStreamingDatasource">
          <template v-if="tabType === 'sample'">
            <el-table
              v-scroll-shadow
              :data="sample.list"
              class="sample-tables"
              ref="sampleTable"
              style="width: 100%">
              <el-table-column
                v-for="item in statistics.list"
                :key="item.id"
                :label="item.name"
                show-overflow-tooltip
                :prop="item.name"
                width="200"
              >
              </el-table-column>
            </el-table>
            <el-pagination v-if="sample.totalSize" class="ksd-center ksd-mtb-10" ref="dataSamplePager" layout="total, sizes, prev, pager, next, jumper" :total="sample.totalSize" :page-size="sample.pageSize" :current-page="sample.pageOffset + 1" @size-change="(val) => pageSampleChange('size', val)" @current-change="(val) => pageSampleChange('page', val)"></el-pagination>
          </template>
        </el-tab-pane>
      </el-tabs>
    </div>
  </div>
</template>
<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import { handleError } from 'util/business'
import { handleSuccessAsync } from 'util'
import locales from './locales'
import TreeList from '../../../../common/TreeList/index'

@Component({
  props: {
    data: {
      type: Object,
      default () {
        return {}
      }
    }
  },
  methods: {
    ...mapActions({
      loadDataSourceByModel: 'LOAD_DATASOURCE_OF_MODEL',
      fetchTables: 'FETCH_TABLES'
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  components: {
    TreeList
  },
  locales
})
export default class DataFeatures extends Vue {
  dbList = []
  tabType = 'dataFeatures'
  statistics = {
    list: [],
    allList: [],
    pageSize: 10,
    totalSize: 0,
    pageOffset: 0
  }
  sample = {
    list: [],
    allList: [],
    pageSize: 10,
    totalSize: 0,
    pageOffset: 0
  }
  defaultCheckKeys = []
  loadingData = false
  isStreamingDatasource = false

  created () {
    this.initData()
  }

  initData () {
    const { alias } = this.data
    this.loadDataSourceByModel({ project: this.currentSelectedProject, model_name: alias }).then(async (response) => {
      const result = await handleSuccessAsync(response)
      const dbs = {}
      result.forEach((item) => {
        if (item.database in dbs) {
          dbs[item.database].children.push({
            label: item.name,
            db: item.database,
            id: `${item.database}.${item.name}`,
            source_type: item.source_type,
            type: 'table'
          })
        } else {
          dbs[item.database] = {
            children: [
              {
                label: item.name,
                db: item.database,
                id: `${item.database}.${item.name}`,
                source_type: item.source_type
              }
            ],
            source_type: item.source_type,
            type: 'table'
          }
        }
      })
      this.dbList = Object.keys(dbs).map(it => ({
        label: it,
        children: dbs[it].children,
        source_type: dbs[it].source_type,
        type: 'database',
        render: (h, {node, data}) => {
          return <span><i class="el-ksd-icon-data_base_16 icon-items"></i>{node.label}</span>
        }
      }))
      this.handleNodeClick(this.dbList[0].children[0])
      this.$nextTick(() => {
        this.$refs['data_features-trees'] && this.$refs['data_features-trees'].$refs.tree.setCurrentNode(this.dbList[0].children[0])
      })
    }).catch((e) => {
      handleError(e)
    })
  }

  handleNodeClick (data, node) {
    const { label, db, source_type } = data
    // todo 识别 kafka 数据源的字段标识
    this.isStreamingDatasource = source_type === 1
    this.statistics.pageOffset = 0
    this.sample.pageOffset = 0
    this.loadingData = true
    this.fetchTables({
      projectName: this.currentSelectedProject,
      databaseName: db,
      sourceType: source_type,
      tableName: label,
      isExt: true,
      isFuzzy: false
    }).then(async res => {
      const result = await handleSuccessAsync(res)
      if (!result.tables.length) return
      this.loadingData = false
      this.statistics.allList = result.tables[0].columns
      this.statistics.list = this.statistics.allList.slice(this.statistics.pageOffset, this.statistics.pageSize)
      this.statistics.totalSize = this.statistics.allList.length
      this.sample.allList = result.tables[0].sampling_rows.length ? result.tables[0].sampling_rows.map((item) => ({...this.statistics.list.reduce((t, it, index) => ({...t, [it.name]: item[index]}), {})})) : []
      this.sample.list = this.sample.allList.slice(this.sample.pageOffset, this.sample.pageSize)
      this.sample.totalSize = this.sample.allList.length
      this.$nextTick(() => {
        this.tabType === 'dataFeatures' ? (this.$refs.dataFeatureTable && this.$refs.dataFeatureTable.doLayout()) : (this.$refs.sampleTable && this.$refs.sampleTable.doLayout())
      })
    }).catch((e) => {
      this.loadingData = false
      handleError(e)
    })
  }

  // 切换数据特征 tab
  handleTabClick () {
    this.$nextTick(() => {
      this.tabType === 'dataFeatures' ? (this.$refs.dataFeatureTable && this.$refs.dataFeatureTable.doLayout()) : (this.$refs.sampleTable && this.$refs.sampleTable.doLayout())
    })
  }

  // 特征数据分页
  pageStatisticsChange (type, val) {
    if (type === 'size') {
      this.statistics.pageOffset = 0
      this.statistics.pageSize = val
    } else {
      this.statistics.pageOffset = val - 1
    }
    this.$nextTick(() => {
      this.statistics.list = this.statistics.allList.slice(this.statistics.pageSize * this.statistics.pageOffset, this.statistics.pageSize * (this.statistics.pageOffset + 1))
    })
  }

  // 采样数据分页
  pageSampleChange (type, val) {
    if (type === 'size') {
      this.sample.pageOffset = 0
      this.sample.pageSize = val
    } else {
      this.sample.pageOffset = val - 1
    }
    this.$nextTick(() => {
      this.sample.list = this.sample.allList.slice(this.sample.pageSize * this.sample.pageOffset, this.sample.pageSize * (this.sample.pageOffset + 1))
    })
  }
}
</script>
<style lang="less">
@import '../../../../../assets/styles/variables.less';
.data_features-layout {
  height: 100%;
  .el-row {
    .el-col {
      padding: 0;
    }
    .data_features-database.el-col {
      padding: 24px;
    }
  }
  .data_features-details {
    padding: 24px;
    height: 100%;
    width: calc(~'100% - 214px');
    box-sizing: border-box;
    overflow: auto;
    display: inline-block;
    margin-left: -5px;
    .data_features-tabs {
      height: 100%;
      .el-tabs__nav-scroll {
        background-color: @ke-background-color-white;
      }
      .el-tabs__content {
        height: calc(~'100% - 50px');
        overflow: auto;
      }
    }
  }
  .data_features-database {
    width: 214px;
    height: 100%;
    display: inline-block;
    overflow: auto;
    box-sizing: border-box;
    border: 1px solid @ke-border-divider-color;
    border-top: 0;
    vertical-align: top;
    .el-col {
      padding: 10px 0;
    }
  }
  .data_features-tables, .sample-tables {
    .null-value {
      color: @text-secondary-color;
      font-size: 12px;
    }
  }
  .data_features-trees {
    .el-tree-node {
      &.is-current {
        background-color: @ke-background-color-secondary;
      }
      .icon-items {
        margin-right: 3px;
      }
    }
  }
}
</style>
