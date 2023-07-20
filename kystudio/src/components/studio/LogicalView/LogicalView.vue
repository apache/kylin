<template>
    <div class="logical-view-container">
      <div class="left-layout">
        <div class="header">
          <span class="title">{{$t('newLogicalView')}}</span>
        </div>
        <el-alert type="warning" show-icon v-if="showCreateSuccessAlert"><span slot="title">{{$t('createViewSuccessAlert', {databaseName: logicalViewDatabaseName})}} <a class="import-link" href="javascript:void(0);" @click="importDataSource">{{$t('goToImport')}}</a></span></el-alert>
        <div class="editor-content">
          <editor class="ddl-editor" v-model="content" :height="editorHeight" ref="ddlEditor" lang="sql" theme="chrome" @keydown.meta.enter.native="runSql" @keydown.ctrl.enter.native="runSql"></editor>
          <div class="run-btn">
            <el-tooltip effect="dark" placement="left">
              <div slot="content">{{$t('runBtnTip')}}<span class="accelerator-key">{{$t('acceleratorKey')}}</span></div>
              <el-button :loading="running" type="primary" icon-button size="big" icon="el-ksd-n-icon-play-filled" @click="runSql"></el-button>
            </el-tooltip>
          </div>
        </div>
      </div>
      <div :class="['right-layout', {'expand': !!activeType}]">
        <div class="action-btns">
          <el-tooltip :content="$t('logicalView')" effect="dark" placement="left">
            <el-badge is-dot class="sign-item" :hidden="true">
              <el-button :class="{'is-active': activeType === 'logicalView'}" text type="primary" icon-button-mini icon="el-ksd-n-icon-symbol-l-filled" @click="activeType = 'logicalView'"></el-button>
            </el-badge>
          </el-tooltip>
          <el-tooltip :content="$t('datasourceTable')" effect="dark" placement="left">
            <el-badge is-dot class="sign-item" :hidden="true">
              <el-button :class="{'is-active': activeType === 'database'}" text type="primary" icon-button-mini icon="icon el-ksd-n-icon-node-database-filled" @click="activeType = 'database'"></el-button>
            </el-badge>
          </el-tooltip>
          <el-tooltip :content="$t('syntaxRules')" effect="dark" placement="left">
            <el-badge is-dot class="sign-item" :hidden="!ddlError">
              <el-button :class="{'is-active': activeType === 'result'}" text type="primary" icon-button-mini icon="icon el-ksd-n-icon-node-thunder-filled" @click="activeType = 'result'"></el-button>
            </el-badge>
          </el-tooltip>
        </div>
        <div class="panel-content-layout">
          <div class="panel-header" v-if="activeType">
            <span class="title">{{expandBlockTitle}}</span>
            <i class="el-ksd-n-icon-close-L-outlined close-btn" @click="activeType = ''"></i>
          </div>
          <div class="datasource-layout" v-if="activeType === 'logicalView'">
            <data-source-bar
              key="logicalView"
              ref="logicalViewDataSource"
              class="data-source-layout"
              :project-name="currentSelectedProject"
              :is-show-action-group="false"
              :is-show-load-source="false"
              :is-show-load-table="datasourceActions.includes('loadSource') && $store.state.config.platform !== 'iframe'"
              :is-expand-on-click-node="false"
              :is-show-drag-width-bar="true"
              :default-width="240"
              :expand-node-types="['datasource', 'database']"
              :ignore-node-types="['column']"
              :is-logical-view="true"
              :hide-bar-title="$store.state.config.platform === 'iframe'"
              :custom-tree-title="$store.state.config.platform !== 'iframe' ? '' : 'kylinLang.common.dataDirectory'"
              @autoComplete="handleAutoComplete"
              @click="clickTable"
              @edit="editLogicalSql">
            </data-source-bar>
          </div>
          <div class="datasource-layout" v-show="activeType === 'database'">
            <data-source-bar
              key="database"
              ref="ddlDataSource"
              class="data-source-layout"
              :project-name="currentSelectedProject"
              :is-show-action-group="false"
              :is-show-load-source="false"
              :is-show-load-table="datasourceActions.includes('loadSource') && $store.state.config.platform !== 'iframe'"
              :is-expand-on-click-node="false"
              :is-show-drag-width-bar="true"
              :default-width="240"
              :expand-node-types="['datasource', 'database']"
              :hide-bar-title="$store.state.config.platform === 'iframe'"
              :custom-tree-title="$store.state.config.platform !== 'iframe' ? '' : 'kylinLang.common.dataDirectory'"
              @autoComplete="handleAutoComplete"
              @click="clickTable">
            </data-source-bar>
          </div>
          <div class="import-btn" v-if="activeType === 'database'"><el-button @click="importDataSource" type="primary" size="big" icon="el-ksd-n-icon-inport-outlined">{{$t('importDataSource')}}</el-button></div>
          <template v-if="activeType === 'result'">
            <div class="feedback-suggestions">
              <div class="suggestion-list">
                <div class="label">{{$t('createDDLSuggestionTitle')}}</div>
                <el-alert v-if="errorMsg" class="ksd-mb-16" :title="errorMsg" type="error" show-icon :closable="false"></el-alert>
                <div v-if="stacktrace" class="trance-msg">{{stacktrace}}</div>
                <div class="item" v-for="(item, index) in ($lang === 'en' ? rules[0] : rules[1])" :key="index">{{index + 1}}. <span v-html="item.replace(/\n/g, '<br/>')"></span></div>
              </div>
            </div>
          </template>
        </div>
      </div>
      <EditLogicalDialog />
    </div>
  </template>
  <script>
  import { Component, Vue } from 'vue-property-decorator'
  import { mapActions, mapGetters } from 'vuex'
  import DataSourceBar from '../../common/DataSourceBar'
  import { insightKeyword } from '../../../config'
  import { handleSuccessAsync, handleError } from '../../../util'
  import EditLogicalDialog from './EditLogicalDialog/EditLogicalDialog'
  @Component({
    computed: {
      ...mapGetters([
        'currentSelectedProject',
        'datasourceActions',
        'currentProjectData',
        'logicalViewDatabaseName'
      ])
    },
    methods: {
      ...mapActions({
        getDDLDescription: 'DDL_DESCRIPTION',
        runDDL: 'RUN_DDL'
      }),
      ...mapActions('EditLogicalDialog', {
        callEditLogicalDialog: 'CALL_MODAL'
      })
    },
    components: {
      DataSourceBar,
      EditLogicalDialog
    },
    locales: {
      en: {
        newLogicalView: 'New Logical View',
        logicalView: 'Logical View',
        logicalViewTable: 'Logical View Table',
        datasourceTable: 'Data Source Table',
        syntaxRules: 'Syntax Rules',
        createDDLSuggestionTitle: 'Logical View is a special view which only exists in Kylin,you can create or drop Logical View on this page. Create Logical View in Kylin needs to follow the following  rules.',
        importDataSource: 'Import',
        runBtnTip: 'Execute ',
        acceleratorKey: '⌃/⌘ enter',
        runSuccess: 'Execute succeed.',
        runFailed: 'Execute Failed, Please check and try again.',
        createViewSuccessAlert: 'After creating logical view, the view is created in virtual database {databaseName}, you need click the "refresh now" link in datasource and then can see the new Logical View. Then you should load it into data source.',
        goToImport: 'Go to Import'
      }
    }
  })
  export default class DDL extends Vue {
    content = ''
    activeType = 'result'
    rules = []
    ddlError = false
    errorMsg = ''
    stacktrace = ''
    running = false
    showCreateSuccessAlert = false
    get expandBlockTitle () {
      switch (this.activeType) {
        case 'logicalView':
          return this.$t('logicalViewTable')
        case 'database':
          return this.$t('datasourceTable')
        default:
          return this.$t('syntaxRules')
      }
    }
    get editorHeight () {
      if (this.showCreateSuccessAlert) {
        return 'calc(100% + 37px)'
      }
      return '100%'
    }

    setOption (option) {
      let editor = this.$refs.ddlEditor.editor
      if (!editor) return
      editor.setOptions(Object.assign({
        wrap: 'free',
        enableBasicAutocompletion: true,
        enableSnippets: true,
        enableLiveAutocompletion: true
      }, option))
    }
    handleAutoComplete (data) {
      this.completeData = [...data, ...insightKeyword]
    }
    clickTable (leaf) {
      this.$nextTick(() => {
        if (leaf) {
          this.insertEditorContent(leaf.label)
        }
      })
    }
    async editLogicalSql (data, node, event) {
      event.stopPropagation()
      if (!data.isCurrentProLogicalTable) return
      const isSubmit = await this.callEditLogicalDialog({ sql: data.__data.created_sql })
      if (isSubmit) {
        this.$refs.logicalViewDataSource.initTree()
      }
    }
    importDataSource () {
      this.$refs.ddlDataSource && this.$refs.ddlDataSource.importDataSource('selectSource', this.currentProjectData)
    }
    insertEditorContent (data) {
      const editor = this.$refs.ddlEditor.editor
      editor.focus()
      editor.insert(data)
      this.content = editor.getValue()
    }
    async runSql () {
      try {
        this.ddlError = false
        this.running = true
        const res = await this.runDDL({
          sql: this.content,
          ddl_project: this.currentSelectedProject,
          restrict: 'logic'
        })
        const resultData = await handleSuccessAsync(res)
        this.running = false
        const logicalRule = /create\s+logical\s+view/i
        this.showCreateSuccessAlert = logicalRule.test(this.content)
        resultData && this.insertEditorContent(`\n\n${resultData}`)
        this.resetErrorMsg()
        this.$message({ type: 'success', message: this.$t('runSuccess') })
      } catch (e) {
        const err = e.body
        this.ddlError = true
        this.errorMsg = err.msg
        this.stacktrace = err.stacktrace
        this.activeType = 'result'
        this.running = false
        this.showCreateSuccessAlert = false
        this.$message({ type: 'error', message: this.$t('runFailed') })
        // handleError(e)
      }
    }
    resetErrorMsg () {
      this.errorMsg = ''
      this.stacktrace = ''
    }
    async getDDLRules () {
      try {
        const result = await this.getDDLDescription({project: this.currentSelectedProject, page_type: 'logic'})
        const rules = await handleSuccessAsync(result)
        this.rules = rules
      } catch (e) {
        handleError(e)
      }
    }
    created () {
      this.getDDLRules()
    }
    mounted () {
      this.setOption()
    }
  }
  </script>
  
  <style lang="less" scoped>
  @import '../../../assets/styles/variables.less';
  .logical-view-container {
    width: 100%;
    height: 100%;
    display: flex;
    .left-layout {
      flex: 1;
      display: flex;
      flex-direction: column;
      position: relative;
      .editor-content {
        flex: 1;
        height: calc(~'100% - 60px');
        overflow: auto;
        .run-btn {
          z-index: 1;
          width: 40px;
          height: 38px;
          position: absolute;
          right: 16px;
          bottom: 16px;
        }
      }
      .import-link {
        color: #1268FB;
      }
    }
    .right-layout {
      width: 46px;
      transition: width .5s;
      display: flex;
      &.expand {
        width: 330px;
      }
      .sign-item {
        .is-dot {
          right: 7px;
          margin-top: 2px;
        }
      }
      .action-btns {
        width: 46px;
        height: 100%;
        display: flex;
        flex-direction: column;
        align-items: center;
        gap: 8px;
        border-left: 1px solid @ke-border-secondary;
        padding: 8px 8px;
        box-sizing: border-box;
        .el-button {
          color: @text-disabled-color;
          &.is-active {
            background: @ke-background-color-hover !important;
          }
        }
      }
      .feedback-suggestions {
        padding: 16px 8px;
        box-sizing: border-box;
        .suggestion-list {
          .label {
            font-size: 14px;
            font-weight: 400;
            color: @text-disabled-color;
            line-height: 22px;
            margin-bottom: 16px;
          }
          .item {
            font-size: 14px;
            color: @text-title-color;
            line-height: 22px;
            word-break: break-all;
          }
          .trance-msg {
            height: 270px;
            overflow: auto;
            background: @ke-background-color-secondary;
            border: 1px solid @ke-border-secondary;
            border-radius: 4px;
            margin-bottom: 16px;
            padding: 8px 16px;
            box-sizing: border-box;
            word-break: break-word;
          }
        }
      }
      .panel-content-layout {
        height: 100%;
        width: 284px;
        flex: 1;
        display: flex;
        flex-direction: column;
        .panel-header {
          height: 60px;
          padding: 0 8px;
          box-sizing: border-box;
          position: relative;
          .title {
            line-height: 60px;
            font-weight: 600;
            font-size: 16px;
          }
          .close-btn {
            position: absolute;
            line-height: 60px;
            right: 25px;
            cursor: pointer;
          }
        }
        .datasource-layout {
          flex: 1;
          height: 0;
        }
        .data-source-layout {
          overflow: hidden;
        }
        .import-btn {
          height: 70px;
          padding: 16px;
          box-sizing: border-box;
          .el-button {
            width: 100%;
          }
        }
      }
    }
    .header {
      height: 60px;
      display: flex;
      align-items: center;
      padding: 0 16px;
      box-sizing: border-box;
      border-bottom: 1px solid @ke-border-secondary;
      .title {
        font-weight: 600;
        font-size: 16px;
      }
    }
  }
  </style>
  <style lang="less">
  @import '../../../assets/styles/variables.less';
  .ddl-editor {
    .ace_content {
      width: 100% !important;
    }
    .ace_print-margin {
      visibility: hidden !important;
    }
  }
  .panel-content-layout {
    .data-source-bar {
      width: 100% !important;
      height: 100%;
      .body {
        width: 100%;
        height: 100%;
        padding: 0;
        .el-tree {
          border: 0;
        }
      }
      .el-input__inner {
        border: 0;
        box-shadow: none;
      }
    }
  }
  .sign-item {
    .el-badge__content.is-fixed.is-dot {
      right: 7px;
      margin-top: 2px;
    }
  }
  .accelerator-key {
    color: @text-disabled-color;
  }
  </style>
  