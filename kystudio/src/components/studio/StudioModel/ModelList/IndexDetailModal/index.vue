<template>
   <el-dialog
    width="720px"
    class="index-detail-modal"
    :title="$t('dialogType')"
    :append-to-body="true"
    :visible="isShow"
    @close="closeModal"
    :close-on-click-modal="false"
    :close-on-press-escape="false">
    <div class="ksd-list info-detail">
      <p class="list">
        <span class="label">Index ID</span>
        <span class="text">{{infoDetail.id}}</span>
      </p>
      <p class="list">
        <span class="label">{{$t('indexPath')}}</span>
        <span class="text path">{{indexDetail.location || '-'}}</span>
      </p>
      <p class="list">
        <span class="label">{{$t('fileCount')}}</span>
        <span class="text">{{indexDetail.num_of_files || '-'}}</span>
      </p>
      <p class="list">
        <span class="label">{{$t('patitionColumn')}}</span>
        <span class="text">{{indexDetail.partition_columns ? indexDetail.partition_columns.join(',') : '-'}}</span>
      </p>
      <p class="list">
        <span class="label">Z-order By</span>
        <span class="text">{{indexDetail.zorder_by_columns ? indexDetail.zorder_by_columns.join(',') : '-'}}</span>
      </p>
      <p class="list">
        <span class="label">{{$t('maximumFileSize')}}</span>
        <span class="text">{{indexDetail.max_compaction_file_size_in_bytes | dataSize}}</span>
      </p>
      <p class="list">
        <span class="label">{{$t('minimumFileSize')}}</span>
        <span class="text">{{indexDetail.min_compaction_file_size_in_bytes | dataSize}}</span>
      </p>
    </div>
    <div slot="footer" class="dialog-footer ky-no-br-space">
      <el-button
        size="medium"
        @click="closeModal">{{$t('kylinLang.common.close')}}</el-button>
    </div>
  </el-dialog>
</template>
<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapMutations, mapActions, mapGetters } from 'vuex'
import vuex from '../../../../../store'
import locales from './locales'
import store, { types } from './store'
import { handleError, handleSuccessAsync } from '../../../../../util'
vuex.registerModule(['modals', 'IndexDetailModal'], store)
@Component({
  computed: {
    ...mapGetters(['currentSelectedProject']),
    ...mapState('IndexDetailModal', {
      isShow: state => state.isShow,
      callback: state => state.callback,
      infoDetail: state => state.infoDetail
    })
  },
  methods: {
    ...mapActions({
      getIndexDetail: 'GET_INDEX_DETAIL'
    }),
    ...mapMutations('IndexDetailModal', {
      hideModal: types.HIDE_MODAL
    })
  },
  components: {
  },
  locales
})
export default class IndexDetailModal extends Vue {
  indexDetail = {}

  @Watch('isShow')
  async onModalShow (val) {
    if (val) {
      try {
        const res = await this.getIndexDetail({
          project: this.currentSelectedProject,
          model_id: this.infoDetail.model,
          index_id: this.infoDetail.id
        })
        const result = await handleSuccessAsync(res)
        if (result) {
          this.indexDetail = result
        }
      } catch (e) {
        console.log(e)
        handleError(e)
      }
    }
  }

  closeModal () {
    this.hideModal()
  }
}
</script>
<style lang="less">
.index-detail-modal{
  .info-detail{
    .label {
      width:130px;
      text-align: right;
    }
    .path {
      word-break: break-all;
    }
    &.ksd-list .list {
      align-items: baseline;
    }
  }
}

</style>
