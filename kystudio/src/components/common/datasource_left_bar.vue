<template>
  <div class="ksd_left_bar" id="input-inner">
    <div class="datasource-title">
      <span>{{$t('kylinLang.common.dataSource')}}</span>
      <i class="el-icon-ksd-load ksd-fright" @click="openLoadDataSourceDialog"></i>
    </div>
    <div v-if="(isAdmin || hasProjectAdminPermission())&&!tableData.length" class="btn-group">
       <el-button @click.native="openLoadDataSourceDialog" type="primary" plain size="medium" icon="el-icon-ksd-load">{{$t('kylinLang.common.dataSource')}}
       </el-button>
    </div>
    <div class="tree-list">
      <tree class="insight-search" :tableType="tableType" :empty-text="$t('kylinLang.common.dialogHiveTreeNoData')" :expandnodeclick="false" :treedata="tableData" :placeholder="$t('kylinLang.common.pleaseFilter')"  :indent="4" :expandall="false" :showfilter="true" :allowdrag="false" @contentClick="clickTable" maxlevel="4"></tree>
    </div>
    <el-dialog class="load-datasource-dialog" :title="dataSourceLoadDialogTitle" width="720px" limited-area :visible.sync="loadDataSourceVisible" :close-on-press-escape="false" :close-on-click-modal="false">
      <div v-if="activeLoadFormIndex===-1">
        <p class="ksd-center ksd-mt-40 select-title">{{$t('dataSourceTypeCheckTip')}}{{initDefaultCheck}}</p>
       
        <ul class="ksd-center datasource-type">
          <li class="type-hive" @click="checkDataSourceType('0')">
            <div :class="{active:currentUserCheckType=== '0'}"></div>
            <p>Hive</p>
          </li>
          <li class="type-rdbms" @click="checkDataSourceType('16')">
            <div :class="{active:currentUserCheckType==='16' || currentUserCheckType==='8'}"></div>
            <p>RDBMS</p>
          </li>
          <li class="type-kafka" @click="checkDataSourceType('1')">
            <div :class="{active:currentUserCheckType==='1'}"></div>
            <p>Kafka</p>
          </li>
        </ul>
         <p class="ksd-center ksd-mt-20 checksource-warn-msg">{{$t('singleSourceTip')}}</p>
      </div>
      <div v-if="activeLoadFormIndex===0">
        <div style="display:flex;">
         <div class="left-tree-part">
           <div class="dialog_tree_box">
             <tree
             @lazyload="loadChildNode"
             :multiple="true"
             @nodeclick="clickHiveTable"
             :lazy="true"
             :treedata="hiveData"
             :emptyText="dialogEmptyText"
             maxlevel="3"
             ref="subtree"
             :maxLabelLen="20"
             :showfilter="false"
             :allowdrag="false" ></tree>
            </div>
         </div>
         <div class="right-tree-part">
           <div class="tree_check_content ksd-mt-20">
              <arealabel :validateRegex="/^\s*;?(\w+\.\w+)\s*(,\s*\w+\.\w+)*;?\s*$/" @validateFail="selectedHiveValidateFail" @refreshData="refreshHiveData" splitChar=","  :selectedlabels="selectTablesNames" :allowcreate='true' placeholder=" " @removeTag="removeSelectedHive"  :datamap="{label: 'label', value: 'value'}"></arealabel>
          <div class="ksd-mt-22 ksd-extend-tips" v-html="$store.state.system.sourceDefault === '0' ? $t('loadHiveTip') : $t('loadTip')"></div>
          <div class="ksd-mt-20">
            <slider @changeBar="changeBar" :show="load_hive_dalog_visible" class="ksd-mr-20 ksd-mb-20">
              <span slot="checkLabel">{{$t('sampling')}}</span>
               <span slot="tipLabel">
                  <common-tip placement="right" :content="$t('kylinLang.dataSource.collectStatice')" >
                     <i class="el-icon-ksd-what"></i>
                  </common-tip>
               </span>
            </slider>
          </div>
          </div> 
         </div>
        </div>
      </div>

      <div  v-if="activeLoadFormIndex===1" style="padding:20px;">
        <create_kafka  ref="kafkaForm" v-on:validSuccess="kafkaValidSuccess" :show="kafkaFormVisible"></create_kafka>
      </div>

      <div slot="footer" class="dialog-footer">
        <el-button @click="loadDataSourceVisible=false;activeLoadFormIndex===-1" v-if="activeLoadFormIndex===-1">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button @click="activeLoadFormIndex=-1" v-if="!selectedProjectDatasource && activeLoadFormIndex!==-1">{{$t('kylinLang.common.prev')}}</el-button>
        <el-button type="primary" @click="nextLoadForm"  v-if="activeLoadFormIndex===-1">{{$t('kylinLang.common.next')}}</el-button>
        <el-button type="primary" @click="loadHiveList" :loading="loadHiveLoad" v-if="(currentUserCheckType==='0' || currentUserCheckType==='16'  || currentUserCheckType==='8')&& activeLoadFormIndex!==-1">{{$t('kylinLang.common.sync')}}</el-button>
        <el-button type="primary" @click="checkKafkaForm" :loading="kafkaLoading" v-if="currentUserCheckType==='1' && activeLoadFormIndex!==-1">{{$t('kylinLang.common.submit')}}</el-button>
      </div>
    </el-dialog>
  </div>
</template>

<script>
  import { mapActions, mapGetters } from 'vuex'
  import { permissions } from '../../config'
  import { objectClone, objectArraySort } from '../../util/index'
  import { handleSuccess, handleError, hasRole, hasPermission } from '../../util/business'
  import createKafka from '../kafka/create_kafka'
  import arealabel from './area_label'
  import Vue from 'vue'
  import { Component } from 'vue-property-decorator'
  @Component({
    props: ['tableData'],
    methods: {
      ...mapActions({
        loadDataSourceByProject: 'LOAD_DATASOURCE',
        loadDatabase: 'LOAD_HIVEBASIC_DATABASE',
        loadTablesByDatabse: 'LOAD_HIVE_TABLES',
        loadHiveInProject: 'LOAD_HIVE_IN_PROJECT',
        updateProject: 'UPDATE_PROJECT',
        loadAllProjects: 'LOAD_ALL_PROJECT'
      }),
      clickTable (node) {
        this.$emit('clickTable', node)
      },
      openLoadDataSourceDialog () {
        if (!this.project) {
          this.$message(this.$t('kylinLang.project.mustSelectProject'))
          return
        }
        this.openCollectRange = false
        this.tableStaticsRange = 0
        this.loadDataSourceVisible = true
        this.activeLoadFormIndex = -1
        this.selectTables = []
        this.selectTablesNames = []
        this.$nextTick(() => {
          if (this.selectedProjectDatasource) {
            this.nextLoadForm()
          } else {
            this.currentUserCheckType = this.globalDefaultDatasource
          }
        })
      },
      updataProjectDatasource (cb) {
        if (!this.project) {
          this.$message(this.$t('kylinLang.project.mustSelectProject'))
          return
        }
        if (this.selectedProjectDatasource) {
          cb()
          return
        }
        var projectInstance = objectClone(this.currentProjectData)
        projectInstance && (projectInstance.override_kylin_properties['kylin.source.default'] = this.currentUserCheckType)
        this.updateProject({name: this.project, desc: JSON.stringify(projectInstance)}).then((result) => {
          this.loadAllProjects()
          cb()
        }, (res) => {
          handleError(res)
        })
      },
      checkDataSourceType (type) {
        // 如果项目有单独的数据源类型的配置就不让点击
        if (this.selectedProjectDatasource) {
          return
        }
        this.currentUserCheckType = type
      },
      nextLoadForm () {
        this.updataProjectDatasource(() => {
          if (this.currentUserCheckType === '0' || this.currentUserCheckType === '16' || this.currentUserCheckType === '8') {
            this.activeLoadFormIndex = 0
          } else {
            this.activeLoadFormIndex = 1
          }
          this.selectTables = []
          this.selectTablesNames = []
        })
      },
      checkKafkaForm: function () {
        this.$refs['kafkaForm'].$emit('kafkaFormValid')
      },
      kafkaValidSuccess: function (data) {
        // 先更新project的默认数据源
        this.kafkaLoading = true
        let columns = []
        data.columnList.forEach(function (column, $index) {
          if (column.checked === 'Y') {
            let columnInstance = {
              id: ++$index,
              name: column.name,
              datatype: column.type,
              comment: /[|]/.test(column.comment) ? column.comment : '' // 不是嵌套结构的就不传该内容
            }
            columns.push(columnInstance)
          }
        })
        let tableData = {
          name: data.kafkaMeta.name,
          source_type: 1,
          columns: columns,
          database: data.database || 'Default'
        }
        data.streamingMeta.name = data.kafkaMeta.name
        this.saveKafka({
          kafkaConfig: JSON.stringify(data.kafkaMeta),
          streamingConfig: JSON.stringify(data.streamingMeta),
          project: this.project,
          tableData: JSON.stringify(tableData)
        }).then((res) => {
          handleSuccess(res, () => {
            this.kafkaLoading = false
            this.$message({
              type: 'success',
              message: this.$t('kylinLang.common.saveSuccess')
            })
            this.saveSampleData({ tableName: data.database + '.' + data.tableName, sampleData: data.sampleData, project: this.project })
            this.loadDataSourceVisible = false
            this.loadHiveTree()
            // this.showTableDetail(data.database, data.tableName)
          })
        }, (res) => {
          this.kafkaLoading = false
          handleError(res)
        })
      },
      refreshHiveData (val) {
        this.selectTablesNames = val
      },
      removeSelectedHive (val) {
        this.$refs.subtree.cancelNodeChecked(val)
        this.selectTablesNames.splice(this.selectTablesNames.indexOf(val), 1)
        this.selectTables = this.selectTables.filter((t) => {
          return t.id === val
        })
      },
      selectedHiveValidateFail () {
        this.$message(this.$t('selectedHiveValidateFailText'))
      },
      loadHiveList () {
        // 先更新project的默认数据源
        if (this.selectTablesNames.length > 0) {
          this.currentAction = this.$t('load')
          this.loadHiveLoad = true
          this.loadHiveInProject({
            project: this.project,
            tables: this.selectTablesNames.join(','),
            data: {
              ratio: (this.tableStaticsRange / 100).toFixed(2),
              tables: this.selectTablesNames,
              project: this.project,
              needProfile: this.openCollectRange
            }
          }).then((response) => {
            handleSuccess(response, (data) => {
              this.$set(this.loadResult, 'success', data['result.loaded'])
              this.$set(this.loadResult, 'fail', data['result.unloaded'])
              this.$set(this.loadResult, 'running', data['result.running'])
            })
            this.loadHiveTree()
            this.loadDataSourceVisible = false
            this.loadResultVisible = true
            this.selectTables = []
            this.selectTablesNames = []
            this.loadHiveLoad = false
          }, (res) => {
            this.loadHiveLoad = false
            handleError(res)
            this.loadDataSourceVisible = false
          })
        }
      },
      loadHiveTree () {
        return this.loadDataSourceByProject({project: this.project, isExt: true}).then((response) => {
          handleSuccess(response, (data, code) => {
            var datasourceData = data
            var datasourceTreeData = {
              id: '1',
              label: 'Tables',
              children: []
            }
            var databaseData = {}
            for (var i = 0; i < datasourceData.length; i++) {
              databaseData[datasourceData[i].database] = databaseData[datasourceData[i].database] || []
              databaseData[datasourceData[i].database].push(datasourceData[i])
            }
            for (var s in databaseData) {
              var obj = {}
              obj.id = s
              obj.label = s
              obj.children = []

              for (var f = 0; f < databaseData[s].length; f++) {
                var childObj = {}
                childObj.id = s + '$' + databaseData[s][f].name
                childObj.data = databaseData[s][f].name
                childObj.label = databaseData[s][f].name
                childObj.tags = databaseData[s][f].source_type === 1 ? ['S'] : null
                // childObj.checked = true
                obj.children.push(childObj)
              }
              obj.children = objectArraySort(obj.children, true, 'label')
              datasourceTreeData.children.push(obj)
            }
            datasourceTreeData.children = objectArraySort(datasourceTreeData.children, true, 'label')
            this.hiveAssets = datasourceData.length === 0 ? [] : [datasourceTreeData]
          })
        }, (res) => {
          handleError(res)
        })
      },
      clickHiveTable (data, vnode) {
        if (data.id && data.id.indexOf('.') > 0 && !data.isMore) {
          var newArr = this.selectTables.filter(function (item) {
            return item.value === data.id
          })
          if (!newArr || newArr.length <= 0) {
            this.selectTables.push({
              label: data.id,
              value: data.id
            })
            this.selectTablesNames.push(data.id)
          }
        }
        var node = data
        if (node.index) {
          // 加载更多
          vnode.store.remove(vnode.data)
          var addData = node.fullData.slice(0, node.index + this.treePerPage)
          var moreNodes = []
          for (var k = 0; k < addData.length; k++) {
            moreNodes.push({
              id: node.parentLabel + '.' + addData[k],
              label: addData[k]
            })
          }
          var renderChildrens = objectClone(moreNodes)
          if (node.index + this.treePerPage < node.fullData.length) {
            renderChildrens.push({
              id: node.parentLabel + '...',
              label: '。。。',
              parentLabel: node.parentLabel,
              fullData: node.fullData,
              children: [],
              index: node.index + this.treePerPage,
              isMore: true
            })
          }
          node.parentNode.children = renderChildrens
        }
      },
      loadChildNode (node, resolve) {
        if (node.level === 0) {
          return resolve([{label: this.selectedProjectDatasource === '0' ? 'Hive Table' : 'Table'}])
        } else if (node.level === 1) {
          this.loadDatabase(this.$store.state.project.selected_project).then((res) => {
            handleSuccess(res, (data) => {
              var datasourceTreeData = []
              for (var i = 0; i < data.length; i++) {
                datasourceTreeData.push({id: data[i], label: data[i], children: [], fullData: data})
              }
              resolve(datasourceTreeData)
            })
          }, (res) => {
            node.loading = false
            handleError(res)
          })
        } else if (node.level === 2) {
          var subData = []
          this.loadTablesByDatabse({database: node.label, project: this.$store.state.project.selected_project}).then((res) => {
            handleSuccess(res, (data) => {
              var len = data && data.length || 0
              var pagerLen = len > this.treePerPage ? this.treePerPage : len
              for (var k = 0; k < pagerLen; k++) {
                subData.push({
                  id: node.label + '.' + data[k],
                  label: data[k]
                })
              }
              if (pagerLen < len) {
                subData.push({
                  id: node.label + '...',
                  label: '。。。',
                  children: [],
                  parentNode: node,
                  parentLabel: node.label,
                  fullData: data,
                  index: this.treePerPage,
                  isMore: true
                })
              }
              resolve(subData)
            })
          }, (res) => {
            node.loading = false
            handleError(res)
          })
        } else {
          resolve([])
        }
      },
      changeBar (val) {
        this.tableStaticsRange = val
        this.openCollectRange = !!val
      }
    },
    components: {
      arealabel,
      'create_kafka': createKafka
    },
    computed: {
      isAdmin () {
        return hasRole(this, 'ROLE_ADMIN')
      },
      hasProjectAdminPermission () {
        return hasPermission(this, permissions.ADMINISTRATION.mask)
      },
      ...mapGetters([
        'selectedProjectDatasource',
        'globalDefaultDatasource',
        'currentProjectData'
      ]),
      dataSourceLoadDialogTitle () {
        if (this.activeLoadFormIndex === -1) {
          return this.$t('newDataSource')
        } else if (this.currentUserCheckType === '0') {
          this.tableType = 'Hive'
          return this.$t('loadhiveTables')
        } else if (this.currentUserCheckType === '16' || this.currentUserCheckType === '8') {
          this.tableType = 'RDBMS'
          return this.$t('loadTables')
        } else if (this.currentUserCheckType === '1') {
          this.tableType = 'Kafka'
          return this.$t('loadKafkaTopic')
        }
      },
      initDefaultCheck () {
        this.currentUserCheckType = this.selectedProjectDatasource || this.globalDefaultDatasource
        return ''
      },
      tableType () {
        if (this.currentUserCheckType === '0') {
          return 'Hive'
        } else if (this.currentUserCheckType === '16' || this.currentUserCheckType === '8') {
          return 'RDBMS'
        } else if (this.currentUserCheckType === '1') {
          return 'Kafka'
        } else {
          return ''
        }
      }
    },
    locales: {
      'en': {dataSourceTypeCheckTip: 'Please Select Data Source Type', singleSourceTip: 'You can choose one type of data source within the project and it cannot be modified after selection.', newDataSource: 'New Data Source', loadhiveTables: 'Load Hive Table Metadata', loadTables: 'Load Table Metadata', loadKafkaTopic: 'Load Kafka Topic', selectedHiveValidateFailText: 'Please enter table name as \'database.table\'.', dialogHiveTreeLoading: 'loading', loadHiveTip: '<p style="font-weight: bold">HOW TO SYNC TABLE\'S METADATA</p><p class="ksd-mt-10"><span style="font-weight: bold">Select tables from source tree: </span>click tables in the left hive tree and the maximum is 1,000 tables.</p><p><span style="font-weight: bold">Enter table name as \'database.table\': </span>if you don\'t need to take a look at tables, just enter table name as \'database.table\'; use comma to separate multiple tables\' name; use ENTER to close entering. The maximum is 1000 tables.</p>', sampling: 'Table Sampling'},
    }
  })
  export default class DatasourceLeftBar extends Vue {
    data () {
      return {
        project: localStorage.getItem('selected_project'),
        loadDataSourceVisible: false,
        activeLoadFormIndex: -1,
        currentUserCheckType: '',
        tableStaticsRange: 100,
        loadHiveLoad: false,
        openCollectRange: false,
        load_hive_dalog_visible: false,
        dialogEmptyText: this.$t('dialogHiveTreeLoading'),
        selectTables: [],
        selectTablesNames: [],
        hiveData: [],
        loadResultVisible: false,
        loadResult: {
          success: [],
          fail: [],
          running: []
        },
        kafkaLoading: false,
        kafkaFormVisible: false
      }
    }
  }
</script>

<style lang="less">
  @import '../../assets/styles/variables.less';
  .ksd_left_bar {
    .datasource-title {
      padding: 18px 20px;
      font-size: 16px;
      color: @text-title-color;
      border-bottom: 1px solid @line-split-color;
      .el-icon-ksd-load {
        color: @base-color;
      }
    }
    .btn-group {
      text-align: center;
      padding: 18px 0;
    }
    .tree-list {
      padding: 0 20px;
      .tree_box{
        height: 100%;
        margin-top: 16px;
        .tag_D{
          color:@base-color;
          border:solid 1px @base-color;
          border-radius: 100%;
          font-size: 12px;
        }
        .tag_M{
          color:@base-color;
          border:solid 1px @base-color;
          border-radius: 100%;
          font-size: 12px;
        }
        .tag_L{
          // color:@base-color;
          // border:solid 1px @base-color;
          border-radius: 0;
          font-size: 12px;
        }
        .tag_F{
          // color:@base-color;
          // border:solid 1px @base-color;
          border-radius: 0;
          font-size: 12px;
        }
        .tag_PK{
          color:@base-color;
          border:solid 1px @base-color;
          width: 18px;
          border-radius: 4px;
          font-size: 12px;
        }
        .tag_FK{
          color:@base-color;
          border:solid 1px @base-color;
          width: 18px;
          border-radius: 4px;
          font-size: 12px;
        }

      }
    }
  }
  .load-datasource-dialog{
    .el-dialog__body{
        padding: 0;
        .left-tree-part{
          width:218px;
          max-height:600px;
          overflow-y:auto;
          overflow-x:hidden;
        }
        .right-tree-part{
          border-left: solid 1px @line-border-color;
          flex:1;
          padding: 0 20px 20px;
        }
      }
      .el-select__tags{
        max-width: 100%!important;
        max-height: 320px;
      }
      .el-tabs__content{
        overflow-y: auto;
        overflow-x: auto;
      }
    .select-title{
      color:@text-title-color;
      font-weight: @font-medium;
    }
    .checksource-warn-msg {
      margin-bottom: 240px;
    }
    .datasource-type{
      margin-top: 40px;
      li{
        display: inline-block;
        margin-right: 15px;
        margin-left: 15px;
        cursor: pointer;
        div {
          width: 80px;
          height: 80px;
          background-color: @grey-4;
          background-repeat:no-repeat;
          background-position: center;
          margin-bottom: 10px;
          background-size: 50%;
        }
        &.type-hive {
          div{
            background-image: url('../../assets/img/datasource/hive_blue.png');
            background-size: 60%;
            &.active{
              background-color: @base-color;
              background-image: url('../../assets/img/datasource/hive_white.png');
            }
          }
        }
        &.type-rdbms {
          div{
            background-image: url('../../assets/img/datasource/rdbms_blue.png');
            &.active{
              background-color: @base-color;
              background-image: url('../../assets/img/datasource/rdbms_white.png');
            }
          }
        }
        &.type-kafka {
          div{
            background-image: url('../../assets/img/datasource/kafka_blue.png');
            &.active{
              background-color: @base-color;
              background-image: url('../../assets/img/datasource/kafka_white.png');
            }
          }
        }
      }
    }
  }
</style>
