<template>
  <EditableBlock
    :header-content="$t('switchTitle')"
    :is-editable="false"
    :is-reset="false"
    @submit="(scb, ecb) => handleSubmit(scb, ecb)"
    @cancel="handleCancel">
    <template slot-scope="parentScope">
      <el-form>
        <div class="setting-item">
          <span class="setting-label font-medium">{{$t('switchLabel')}}</span>
          <el-switch
            v-model="internal_table_enabled"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')"
            @change="parentScope.handleSubmit">
          </el-switch>
        </div>
        <div class="setting-item setting-desc">{{$t('functionDesc')}}</div>
      </el-form>
    </template>
  </EditableBlock>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import * as types from 'src/store/types'
import locales from './locales'
import { pageRefTags } from '../../../config'
import EditableBlock from '../../common/EditableBlock/EditableBlock.vue'

@Component({
  locales,
  props: {
    project: {
      type: Object,
      default: () => ({})
    }
  },
  methods: {
    ...mapActions({
      updateInternalTableEnabled: types.UPDATE_INTERNAL_TABLE_ENABLED
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProjectInternalTableEnabled'
    ])
  },
  components: {
    EditableBlock
  }
})
export default class SettingInternalTable extends Vue {
  pageRefTags = pageRefTags

  internal_table_enabled = false

  created () {
    this.internal_table_enabled = this.currentSelectedProjectInternalTableEnabled
  }

  handleSubmit (scb, ecb) {
    // 需要二次确认
    this.$confirm(this.$t('confirmPrompt', { status: this.internal_table_enabled ? this.$t('kylinLang.common.ON') : this.$t('kylinLang.common.OFF') }), this.$t('confirmTitle'), {
      confirmButtonText: this.$t('kylinLang.common.submit'),
      cancelButtonText: this.$t('kylinLang.common.cancel'),
      centerButton: true,
      type: 'warning'
    }).then(async () => {
      try {
        await this.updateInternalTableEnabled({ project: this.project.project, internal_table_enabled: this.internal_table_enabled })
        scb()
        this.$emit('reload-setting')
        this.$message({ type: 'success', message: this.$t('kylinLang.common.updateSuccess') })
      } catch (e) {
        ecb()
      }
    }).catch(() => {
      this.handleCancel()
      return ecb()
    })
  }

  handleCancel () {
    this.internal_table_enabled = !this.internal_table_enabled
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';
</style>
