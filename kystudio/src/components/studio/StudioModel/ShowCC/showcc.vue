<template>
  <el-dialog :title="$t('computedDetail')" append-to-body limited-area width="480px" :visible="isShow" :close-on-press-escape="false" :close-on-click-modal="false" @close="isShow && closeModal()">
    <div class="cc-detail-box" v-if="ccDetail">
      <el-table
      :show-header="false"
      border
      :data="ccInfoData"
      style="width: 100%">
      <el-table-column
        prop="ccKey"
        label=""
        width="120">
      </el-table-column>
      <el-table-column
        prop="ccVal"
        label="">
      </el-table-column>
    </el-table>
      <div class="ksd-mt-18 express-box">
        <p>{{$t('kylinLang.dataSource.expression')}}</p>
        <div class="ksd-mt-6">
          <kylin-editor ref="ccSql" height="100" lang="sql" theme="chrome" :readOnly="true" v-model="ccDetail.expression"></kylin-editor>
        </div>
      </div>
    </div>
  </el-dialog>
</template>
<script>
import Vue from 'vue'
import vuex from '../../../../store'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapMutations } from 'vuex'
import locales from './locales'
import store, { types } from './store'
vuex.registerModule(['modals', 'ShowCCDialogModal'], store)
@Component({
  computed: {
    ...mapState('ShowCCDialogModal', {
      isShow: state => state.isShow,
      ccDetail: state => state.form.ccDetail,
      callback: state => state.callback
    })
  },
  methods: {
    ...mapMutations('ShowCCDialogModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM
    })
  },
  locales
})
export default class ShowCCDialogModal extends Vue {
  @Watch('isShow')
  initDialog () {
    if (this.isShow) {
      this.$nextTick(() => {
        this.$refs.ccSql.$emit('setReadOnly')
      })
    }
  }
  get ccInfoData () {
    return [{
      ccKey: this.$t('columnName'),
      ccVal: this.ccDetail.columnName
    }, {
      ccKey: this.$t('returnType'),
      ccVal: this.ccDetail.datatype
    }]
  }
  closeModal (isSubmit) {
    this.hideModal()
    setTimeout(() => {
      this.callback && this.callback(isSubmit)
      this.resetModalForm()
    }, 200)
  }
}
</script>

<style lang="less">
  @import '../../../../assets/styles/variables.less';
  .cc-detail-box {
    .express-box {
      p {
        font-weight: @font-medium;
      }
    }
  }
</style>
