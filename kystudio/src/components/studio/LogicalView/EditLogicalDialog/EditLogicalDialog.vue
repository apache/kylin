<template>
    <el-dialog class="edit-logical-modal"
      width="800px"
      :title="$t('editLogical')"
      :visible="isShow"
      top="10vh"
      :close-on-press-escape="false"
      :close-on-click-modal="false"
      @close="handleClose">
      <el-alert type="tip" :closable="false" class="ksd-mb-4" show-icon>
        <span slot="title">{{$t('replaceTips')}} <a class="import-link" v-if="sql.toLocaleLowerCase().indexOf('create') > -1" href="javascript:void(0);" @click="goToReplace">{{'create -> replace'}}</a></span>
        <span class="ksd-fs-12">{{$t('dropTips')}}</span>
      </el-alert>
      <kap-editor
        width="100%"
        :height="400"
        lang="sql"
        ref="logicalSql"
        theme="chrome"
        :dragable="false"
        :isAbridge="true"
        :value="sql"
        @input="handleChangeSql">
      </kap-editor>
      <div slot="footer" class="dialog-footer ky-no-br-space">
        <el-button size="medium" @click="handleCancel">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" size="medium" @click="handleSubmit" :loading="running">{{$t('kylinLang.common.save')}}</el-button>
      </div>
    </el-dialog>
  </template>
  
  <script>
  import Vue from 'vue'
  import { Component } from 'vue-property-decorator'
  import { mapState, mapMutations, mapActions, mapGetters } from 'vuex'
  import { handleError } from 'util'
  import vuex, { actionTypes } from 'store'
  import locales from './locales'
  import store from './store'
  vuex.registerModule(['modals', 'EditLogicalDialog'], store)
  @Component({
    computed: {
      ...mapGetters([
        'currentSelectedProject'
      ]),
      ...mapState('EditLogicalDialog', {
        sql: state => state.sql,
        isShow: state => state.isShow,
        callback: state => state.callback
      })
    },
    methods: {
      ...mapMutations('EditLogicalDialog', {
        setModal: actionTypes.SET_MODAL,
        hideModal: actionTypes.HIDE_MODAL,
        initModal: actionTypes.INIT_MODAL
      }),
      ...mapActions({
        runDDL: 'RUN_DDL'
      })
    },
    locales
  })
  export default class EditLogicalDialog extends Vue {
    running = false
    handleClose (isSubmit = false) {
      this.hideModal()
      this.callback && this.callback(isSubmit)
    }
    handleCancel () {
      this.handleClose(false)
    }
    goToReplace () {
      const replaceSql = this.sql.replace(/create/i, 'replace')
      this.setModal({ sql: replaceSql })
    }
    handleChangeSql () {
      const val = this.$refs.logicalSql.getValue()
      this.setModal({ sql: val })
    }
    goToDataSource () {
      this.$router.push('/studio/source')
    }
    async handleSubmit () {
      try {
        this.running = true
        await this.runDDL({
          sql: this.sql,
          ddl_project: this.currentSelectedProject,
          restrict: 'replaceLogicalView'
        })
        this.running = false
        this.$message({
          type: 'success',
          message: (
            <div>
              <span>{this.$t('editSuccess') }</span>
              <a href="javascript:void(0)" onClick={() => this.goToDataSource()}>{this.$t('goToDataSource')}</a>
            </div>
          )
        })
        this.handleClose(true)
      } catch (e) {
        handleError(e)
        this.running = false
      }
    }
  }
  </script>
  