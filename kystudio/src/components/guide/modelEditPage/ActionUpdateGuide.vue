<template>
  <div class="update-model-actions-mask" @mousedown.stop>
    <div class="guide-step1" v-show="step === 1">
      <div :class="['model-edit-title', {'has-global-alert': hasGlobalAlert}]"></div>
      <div :class="['title-tip', {'has-global-alert': hasGlobalAlert}]">
        <div class="title">
          <i class="info-icon el-ksd-n-icon-info-circle-filled"></i>
          <span>{{$t('actionGuideTitle')}}</span>
          <i class="close el-ksd-n-icon-close-outlined" @click="closeGuide"></i>
        </div>
        <p class="content">{{$t('step1Msg')}}</p>
        <div class="footer">
          <span class="step-num">{{`${step}/5`}}</span>
          <el-button type="primary" @click.stop="nextStep">{{$t('kylinLang.common.next')}}</el-button>
        </div>
      </div>
    </div>
    <div class="guide-step5" v-show="step === 2">
      <div class="model-actions"></div>
      <div class="title-tip">
        <div class="title">
          <i class="info-icon el-ksd-n-icon-info-circle-filled"></i>
          <span>{{$t('actionGuideTitle')}}</span>
          <i class="close el-ksd-n-icon-close-outlined" @click="closeGuide"></i>
        </div>
        <p class="content">{{$t('step2Msg')}}</p>
        <div class="footer">
          <span class="step-num">{{`${step}/5`}}</span>
          <div>
            <el-button @click.stop="prevStep" v-if="step > 1">{{$t('kylinLang.common.back')}}</el-button>
            <el-button type="primary" @click.stop="nextStep">{{$t('kylinLang.common.next')}}</el-button>
          </div>
        </div>
      </div>
    </div>
    <el-dialog
      append-to-body
      class="guide-layout"
      :visible="showDialog"
      width="600px"
      :modal="false"
      status-icon="el-ksd-n-icon-info-color-filled"
      :close-on-click-modal="false"
      :before-close="closeGuide">
      <span slot="title" class="guide-title">{{$t('actionGuideTitle')}}</span>
      <div class="step guide-step2" v-if="step === 3">
        <p class="content">{{$t('step3Msg')}}</p>
        <img src="../../../assets/img/guide/model_edit_step1.jpg" width="200px" alt="guid_1">
      </div>
      <div class="step guide-step3" v-if="step === 4">
        <p class="content">{{$t('step4Msg')}}</p>
        <img src="../../../assets/img/guide/model_edit_step3.jpg" width="396px" alt="guid_2">
      </div>
      <div class="step guide-step4" v-if="step === 5">
        <p class="content">{{$t('step5Msg')}}</p>
        <img src="../../../assets/img/guide/model_edit_step2.png" width="450px" alt="guid_3">
      </div>
      <span slot="footer" class="guide-footer">
        <span class="step-num">{{`${step}/5`}}</span>
        <el-button @click.stop="prevStep" v-if="step > 1">{{$t('kylinLang.common.back')}}</el-button>
        <el-button type="primary" @click.stop="nextStep">{{$t('kylinLang.common.next')}}</el-button>
      </span>
    </el-dialog>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
@Component({
  locales: {
    'en': {
      actionGuideTitle: 'Welcome to our new modeling canvas!',
      step1Msg: 'You can now get a quick overview of the model\'s basic information, search content, and operations here.',
      step2Msg: 'Here you can control the size, the full screen, and the view of the canvas.',
      step3Msg: 'You can have an overview of the number of columns included in each table, and which column is used as the primary key (PK) and foreign key (FK).',
      step4Msg: 'You can expand or collapse a table by selecting the respective option on the menu, or double-clicking on the header.',
      step5Msg: 'All columns used for relation are pinned at the top. By hovering on the column, the associated tables and columns will be highlighted.'
    }
  }
})
export default class ActionUpdateGuide extends Vue {
  step = 0
  showDialog = false
  get hasGlobalAlert () {
    const alterDom = document.getElementsByClassName('alter-block')
    return alterDom.length > 0
  }
  mounted () {
    setTimeout(() => {
      this.step = 1
      const modelTitleDom = document.getElementsByClassName('model-edit-header')[0]
      const modelActionDom = document.getElementsByClassName('shortcuts-group')[0]
      const cloneModelTitle = modelTitleDom.cloneNode(true)
      const cloneModelActionDom = modelActionDom.cloneNode(true)
      const dom = this.$el.querySelector('.model-edit-title')
      const guideActions = this.$el.querySelector('.model-actions')
      dom.appendChild(cloneModelTitle)
      guideActions.appendChild(cloneModelActionDom)
    }, 500)
  }
  // 下一步
  nextStep () {
    if (this.step < 5) {
      ++this.step
      if (this.step > 2) {
        this.showDialog = true
      } else {
        this.showDialog = false
      }
    } else {
      this.closeGuide()
    }
  }
  // 上一步
  prevStep () {
    this.step -= 1
    if (this.step > 2) {
      this.showDialog = true
    } else {
      this.showDialog = false
    }
  }
  closeGuide () {
    this.$emit('close-guide')
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';
.update-model-actions-mask {
  z-index: 2000;
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(0, 0, 0, .8);
  cursor: default;
  .model-edit-title {
    background: #fff;
    top: 50px;
    right: 0;
    position: absolute;
    width: calc(~'100% - 180px');
    box-sizing: border-box;
    pointer-events: none;
    &.has-global-alert {
      top: 90px;
    }
  }
  .title-tip.has-global-alert {
    top: 200px;
  }
  .model-actions {
    pointer-events: none;
  }
  .title-tip {
    width: 366px;
    height: 156px;
    border-radius: 12px;
    background: @fff;
    position: absolute;
    top: 160px;
    left: 200px;
    padding: 0 17px 17px 17px;
    box-sizing: border-box;
    display: flex;
    flex-direction: column;
    &::after {
      content: '';
      border-top: 0;
      border-left: 15px solid transparent;
      border-bottom: 15px solid #fff;
      border-right: 15px solid transparent;
      position: absolute;
      top: -14px;
      left: 50%;
      transform: translate(-50%, 0);
    }
    .title {
      font-size: 12px;
      font-weight: 500;
      height: 40px;
      line-height: 40px;
      .info-icon {
        color: #0875DA;
        font-size: 14px;
      }
      .close {
        position: absolute;
        top: 13px;
        right: 17px;
        font-size: 14px;
        cursor: pointer;
      }
    }
    .content {
      flex: 1;
      font-size: 14px;
    }
    .footer {
      display: flex;
      align-items: center;
      justify-content: space-between;
      .step-num {
        color: #8B99AE;
        font-size: 14px;
      }
    }
  }
  .guide-step5 {
    .title-tip {
      bottom: 100px;
      right: 0;
      top: auto;
      left: auto;
      &::after {
        top: auto;
        bottom: -14px;
        transform: rotate(180deg) translate(10px, 0);
      }
    }
  }
}
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