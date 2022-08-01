<template>
  <div class="source-management">
    <h1 class="title">{{$t('sourceManagement')}}</h1>
    <el-table :data="sourceArray">
      <el-table-column prop="name" :label="$t('name')" ></el-table-column>
      <el-table-column prop="type" :label="$t('type')" width="80"></el-table-column>
      <el-table-column prop="createTime" width="218" :label="$t('createTime')">
        <template slot-scope="scope">
          <span>{{scope.row.createTime | timeFormatHasTimeZone}}</span>
        </template>
      </el-table-column>
      <el-table-column :label="$t('kylinLang.common.action')" width="87" class-name="ky-hover-icon">
        <template slot-scope="scope">
          <!-- <el-tooltip :content="$t('batchLoad')" effect="dark" placement="top">
            <i class="el-icon-ksd-batch_check ksd-fs-14 ksd-mr-10" @click="handleBatchLoad"></i>
          </el-tooltip><span>
          </span> -->
          <el-tooltip :content="$t('general')" effect="dark" placement="top">
            <i class="el-icon-ksd-setting ksd-fs-14 ksd-mr-10 disabled" @click="() => false && handleSourceGeneral(scope.row)"></i>
          </el-tooltip><span>
          </span><el-tooltip :content="$t('removeSource')" effect="dark" placement="top">
            <i class="el-icon-ksd-remove_source ksd-fs-14 disabled" @click="() => {}"></i>
          </el-tooltip>
        </template>
      </el-table-column>
    </el-table>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapActions } from 'vuex'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import { handleError } from '../../../../util'
import { mockDatasourceArray } from './mock'

@Component({
  props: {
    project: {
      type: Object
    }
  },
  methods: {
    ...mapActions('BatchLoadModal', {
      callBatchLoadModal: 'CALL_MODAL'
    }),
    ...mapActions('DataSourceModal', {
      callDataSourceModal: 'CALL_MODAL'
    })
  },
  locales
})
export default class SourceManagement extends Vue {
  sourceArray = []
  tableArray = []
  isBatchLoadShow = false
  async mounted () {
    try {
      await this.loadSource()
    } catch (e) {
      handleError(e)
    }
  }
  loadSource () {
    this.sourceArray = mockDatasourceArray
  }
  async handleBatchLoad () {
    const { project } = this
    const isSubmit = await this.callBatchLoadModal({ project })
    isSubmit && this.$emit('fresh-tables')
  }
  async handleSourceGeneral (datasource) {
    const { project } = this
    const isSubmit = await this.callDataSourceModal({ editType: 'viewSource', project, datasource })
    isSubmit && this.$emit('fresh-tables')
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';
.source-management {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  background: @fff;
  z-index: 4;
  padding: 20px;
  .title {
    font-size: 14px;
    color: @text-title-color;
    margin-bottom: 10px;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .disabled {
    color: @text-disabled-color;
    cursor: not-allowed;
  }
}
</style>
