<template>
  <el-dialog class="model-er-diagram-modal"
    width="1000px"
    :title="$t('erDiagram')"
    :visible="isShow"
    :close-on-press-escape="false"
    :close-on-click-modal="false"
    @open="handleOpen"
    @close="handleClose"
    @closed="handleClosed">
    <ModelERDiagram is-show-actions v-if="isBodyShow" :model="model" />
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapState, mapMutations } from 'vuex'

import store from './store'
import locales from './locales'
import ModelERDiagram from '../ModelERDiagram/ModelERDiagram'
import vuex, { actionTypes } from '../../../store'

vuex.registerModule(['modals', 'ModelERDiagramModal'], store)

@Component({
  components: {
    ModelERDiagram
  },
  computed: {
    ...mapState('ModelERDiagramModal', {
      model: state => state.model,
      isShow: state => state.isShow,
      callback: state => state.callback
    })
  },
  methods: {
    ...mapMutations('ModelERDiagramModal', {
      setModalForm: actionTypes.SET_MODAL_FORM,
      setModal: actionTypes.SET_MODAL,
      hideModal: actionTypes.HIDE_MODAL,
      initModal: actionTypes.INIT_MODAL
    })
  },
  locales
})
export default class ModelERDiagramModal extends Vue {
  isBodyShow = false

  handleOpen () {
    this.isBodyShow = true
  }

  handleClose (isSubmit = false) {
    this.hideModal()
    this.callback && this.callback(isSubmit)
  }

  handleClosed () {
    this.isBodyShow = false
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.model-er-diagram-modal {
  .el-dialog__body {
    height: 650px;
    padding: 0;
  }
}
</style>
