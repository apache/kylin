<template>
  <el-dialog
    append-to-body
    class="guide-layout"
    :visible="true"
    width="600px"
    status-icon="el-ksd-n-icon-info-color-filled"
    :close-on-click-modal="false"
    :before-close="closeGuide">
    <span slot="title" class="guide-title">{{$t('actionGuideTitle')}}</span>
    <div class="step guide-step1" v-if="step === 1">
      <p class="content">{{$t('step1Msg')}}</p>
      <img src="../../../assets/img/guide/model_edit_step1.jpg" width="200px" alt="guid_1">
    </div>
    <div class="step guide-step2" v-if="step === 2">
      <p class="content">{{$t('step2Msg')}}</p>
      <img src="../../../assets/img/guide/model_edit_step2.png" width="396px" alt="guid_2">
    </div>
    <div class="step guide-step3" v-if="step === 3">
      <p class="content">{{$t('step3Msg')}}</p>
      <img src="../../../assets/img/guide/model_edit_step3.jpg" width="450px" alt="guid_3">
    </div>
    <span slot="footer" class="guide-footer">
      <span class="step-num">{{`${step}/3`}}</span>
      <el-button @click.stop="--step" v-if="step > 1">{{$t('kylinLang.common.back')}}</el-button>
      <el-button type="primary" @click.stop="nextStep">{{$t('kylinLang.common.next')}}</el-button>
    </span>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
@Component({
  locales: {
    'en': {
      actionGuideTitle: 'Welcome to our new table for modeling canvas!',
      step1Msg: 'You can have an overview of the number of columns included in each table, and which column is used as the primary key (PK) and foreign key (FK).',
      step2Msg: 'You can expand or collapse a table by selecting the respective option on the menu, or double-clicking on the header.',
      step3Msg: 'All columns used for relation are pinned at the top. By hovering on the column, the associated tables and columns will be highlighted.'
    },
    'zh-cn': {
      actionGuideTitle: '欢迎使用我们建模画布的新表！',
      step1Msg: '我们优化了表的信息布局，现在，你可以更清晰的了解列数量和主外键（FK/PK）信息。',
      step2Msg: '我们增加了展开/收起功能，现在，你可以通过双击表头或下拉菜单快速将表展开收起。',
      step3Msg: '我们优化了列的关联显示，现在，你可以在置顶看到所有关联列，通过将鼠标悬浮，有关联关系的表及列将被高亮显示。'
    }
  }
})
export default class ActionUpdateGuide extends Vue {
  step = 1
  nextStep () {
    if (this.step < 3) {
      ++this.step
    } else {
      this.closeGuide()
    }
  }
  closeGuide () {
    this.$emit('close-guide')
  }
}
</script>

<style lang="less">
.guide-layout {
  .guide-title {
    vertical-align: middle;
  }
  .el-dialog .el-dialog__footer {
    position: relative;
  }
  .step {
    text-align: center;
    .content {
      font-size: 14px;
      color: #546174;
      margin-bottom: 16px;
      text-align: left;
    }
  }
  .step-num {
    position: absolute;
    left: 24px;
    top: 30px;
    color: #8B99AE;
    font-size: 14px;
  }
}
</style>