<template>
  <div>
    <el-dialog :title="$t('kylinLang.query.saveQuery')" :visible.sync="saveQueryFormVisible" limited-area width="720px" append-to-body  @close="closeSaveQueryDialog" :close-on-press-escape="false" :close-on-click-modal="false">
      <el-form @submit.native.prevent :model="saveQueryMeta" label-position="top" ref="saveQueryForm" :rules="rules" label-width="85px">
        <el-form-item :label="$t('kylinLang.query.querySql')" prop="sql">
          <kylin-editor height="130" lang="sql" theme="chrome" :readOnly="true" v-model="saveQueryMeta.sql" dragbar="#393e53" :isAbridge="true">
          </kylin-editor>
        </el-form-item>
        <el-form-item :label="$t('kylinLang.query.name')" prop="name">
          <el-input v-model="saveQueryMeta.name" size="medium" auto-complete="off"></el-input>
        </el-form-item>
        <el-form-item :label="$t('kylinLang.query.desc')" prop="description">
          <el-input v-model="saveQueryMeta.description" size="medium" type="textarea"></el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button size="medium" @click="closeSaveQueryDialog">{{$t('kylinLang.common.cancel')}}</el-button><el-button
        size="medium" type="primary" :loading="isSubmit" @click="saveQuery">{{$t('kylinLang.common.submit')}}</el-button>
      </div>
    </el-dialog>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapActions } from 'vuex'
import { NamedRegex } from 'config'
import { handleError } from '../../util/business'
@Component({
  props: ['show', 'project', 'sql'],
  methods: {
    ...mapActions({
      saveQueryToServer: 'SAVE_QUERY'
    })
  }
})
export default class saveQueryDialog extends Vue {
  saveQueryFormVisible = false
  isSubmit = false
  rules = {
    name: [
      { required: true, validator: this.checkName, trigger: 'blur' }
    ]
  }
  saveQueryMeta = {
    name: '',
    description: '',
    project: this.project,
    sql: this.sql
  }

  saveQuery () {
    this.$refs['saveQueryForm'].validate((valid) => {
      if (valid) {
        this.isSubmit = true
        this.saveQueryToServer(this.saveQueryMeta).then((response) => {
          this.isSubmit = false
          this.$message({type: 'success', message: this.$t('kylinLang.common.saveSuccess')})
          this.closeSaveQueryDialog(true)
        }, (res) => {
          this.isSubmit = false
          handleError(res)
          this.closeSaveQueryDialog()
        })
      }
    })
  }
  closeSaveQueryDialog (needRefreshData) {
    this.saveQueryMeta.name = ''
    this.saveQueryMeta.description = ''
    this.saveQueryFormVisible = false
    this.$emit('closeModal', needRefreshData)
  }
  checkName (rule, value, callback) {
    if (!value) {
      callback(new Error(this.$t('kylinLang.common.pleaseInput')))
    } else if (!NamedRegex.test(value)) {
      callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
    } else {
      callback()
    }
  }

  @Watch('show')
  onShowChange (v) {
    this.saveQueryFormVisible = v
    this.saveQueryMeta.project = this.project
    this.saveQueryMeta.sql = this.sql
  }
}
</script>
