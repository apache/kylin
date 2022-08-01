<template>
  <div class="model-guide-mask" v-if="isShow">
    <div class="background"></div>
    <div class="dim-meas-block" :class="{'is-show-global-alter': $store.state.system.isShowGlobalAlter}" v-if="isShowDimAndMeasGuide">
      <div class="tool-icon-group ksd-fright">
        <div class="tool-icon">D</div>
        <div class="tool-icon">M</div>
      </div>
      <div class="desc-block arrow-right ksd-fright">
        <div class="ksd-title-label-small ksd-mb-10">{{$t('dimMeasTitle')}}</div>
        <div>{{$t('dimMeasDesc')}}</div>
        <el-button class="ksd-fright" type="primary" text @click="closeModal">{{$t('iKnow')}}</el-button>
      </div>
    </div>
    <div class="build-guide-block" :class="{'en-mode': $lang === 'en', 'brief-Menu': briefMenuGet, 'is-show-global-alter': $store.state.system.isShowGlobalAlter}" v-if="isShowBuildGuide">
      <div class="segment-block" v-if="step==='step1'">
        <div class="segment-tag">Segment</div>
        <div class="segment-desc arrow-top">
          <div class="ksd-title-label ksd-mb-10">{{$t('segmentTitle')}}</div>
          <div>{{$t('segmentSubTitle')}}</div>
          <div class="ksd-center">
            <img src="../../../../../assets/img/image-seg.png" width="400px" alt="">
          </div>
          <div class="btn-group clearfix">
            <el-button class="ksd-fleft" type="info" text @click="closeModal">{{$t('ignore')}}</el-button>
            <el-button class="ksd-fright" type="primary" text @click="closeModal">{{$t('iKnow')}}</el-button>
            <el-button class="ksd-fright" type="info" text @click="next('step3')">{{$t('pre')}}</el-button>
          </div>
        </div>
      </div>
      <div class="index-block" v-if="step==='step2'">
        <div class="index-tags">
          <span>{{$t('indexOverview')}}</span>
          <span>{{$t('aggregateGroup')}}</span>
          <span>{{$t('tableIndex')}}</span>
        </div>
        <div class="index-desc arrow-top">
          <div class="ksd-title-label ksd-mb-10">{{$t('indexTitle')}}<span v-if="!isStreamingModel">1/3</span></div>
          <div>{{$t('indexSubTitle')}}</div>
          <div class="ksd-center">
            <img src="../../../../../assets/img/image-index-en.png" v-if="$lang === 'en'" width="400px" alt="">
            <img src="../../../../../assets/img/image-index-cn.png" v-else width="400px" alt="">
          </div>
          <div class="btn-group clearfix">
            <el-button class="ksd-fleft" type="info" text @click="closeModal">{{$t('ignore')}}</el-button>
            <el-button class="ksd-fright" type="primary" v-if="isStreamingModel" text @click="closeModal">{{$t('iKnow')}}</el-button>
            <el-button class="ksd-fright" type="primary" v-else text @click="next('step3')">{{$t('next')}}</el-button>
          </div>
        </div>
      </div>
      <div class="build-block" v-if="step==='step3'">
        <div class="build-icon">
          <i class="el-icon-ksd-icon_build-index"></i>
        </div>
        <div class="build-desc arrow-top">
          <div class="ksd-title-label ksd-mb-10">{{$t('buildTitle')}}</div>
          <div>{{$t('buildSubTitle')}}</div>
          <div class="ksd-center">
            <img src="../../../../../assets/img/Seg-index.gif" width="400px" height="300px" alt="">
          </div>
          <div class="btn-group clearfix">
            <el-button class="ksd-fleft" type="info" text @click="closeModal">{{$t('ignore')}}</el-button>
            <el-button class="ksd-fright" type="primary" text @click="next('step1')">{{$t('next')}}</el-button>
            <el-button class="ksd-fright" type="info" text @click="next('step2')">{{$t('pre')}}</el-button>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapState, mapMutations, mapActions, mapGetters } from 'vuex'
import vuex from '../../../../../store'
import locales from './locales'
import store, { types } from './store'
vuex.registerModule(['modals', 'GuideModal'], store)
@Component({
  computed: {
    ...mapState('GuideModal', {
      isShow: state => state.isShow,
      isShowDimAndMeasGuide: state => state.isShowDimAndMeasGuide,
      isShowBuildGuide: state => state.isShowBuildGuide,
      isStreamingModel: state => state.isStreamingModel,
      callback: state => state.callback
    }),
    ...mapGetters([
      'briefMenuGet'
    ])
  },
  methods: {
    ...mapActions({
    }),
    ...mapMutations('GuideModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM
    })
  },
  locales
})
export default class GuideModal extends Vue {
  step = 'step2'
  next (step) {
    this.step = step
  }
  closeModal () {
    this.step = 'step2'
    this.callback && this.callback()
    this.hideModal()
  }
}
</script>

<style lang="less">
@import '../../../../../assets/styles/variables.less';
.model-guide-mask {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: -5px;
  z-index: 999999;
  .background {
    width: 100%;
    height: 100%;
    opacity: 0.7;
    background-color: @000;
  }
  .dim-meas-block {
    position: absolute;
    top:86px;
    right: 0px;
    &.is-show-global-alter {
      top:142px;
    }
    .tool-icon-group {
      position:absolute;
      width:32px;
      top:0px;
      right:0px;
      padding: 10px 10px 0 10px;
      background-color: @color-grey-3;
      border-radius: 5px;
      .tool-icon {
        position:absolute;
        width: 32px;
        height:32px;
        text-align: center;
        line-height: 32px;
        border-radius: 50%;
        box-shadow: @box-shadow;
        background:@text-normal-color;
        color:#fff;
        position:relative;
        margin-bottom: 10px;
        font-weight: @font-medium;
        font-size: 16px;
      }
    }
    .desc-block {
      background-color: rgba(255,255,255,1);
      padding: 20px 20px 10px 20px;
      position: absolute;
      right: 80px;
      width: 250px;
      border-radius: 2px;
      box-shadow:0px 2px 4px 0px rgba(178,178,178,1);
      &.arrow-right::after {
        content: '';
        position: absolute;
        right: -6px;
        top: 20px;
        width: 0;
        height: 0;
        border-bottom: 6px solid transparent;
        border-top: 6px solid transparent;
        border-left: 6px solid rgba(255,255,255,1);
        font-size: 0;
        line-height: 0;
      }
    }
  }
  .build-guide-block {
    .segment-block {
      position: absolute;
      top: 202px;
      left: 134px;
      .segment-tag {
        position: absolute;
        left: 55px;
        top: 0px;
        background-color: rgba(255,255,255,1);
        border-radius: 5px;
        color: @color-primary;
        width: 100px;
        height: 30px;
        line-height: 30px;
        text-align: center;
      }
      .segment-desc {
        position: absolute;
        left: 180px;
        top: -20px;
        width: 500px;
        padding: 20px 20px 10px 20px;
        border-radius: 2px;
        background-color: rgba(255,255,255,1);
        &.arrow-top::after {
          content: '';
          position: absolute;
          left: -6px;
          top: 26px;
          width: 0;
          height: 0;
          border-right: 6px solid rgba(255,255,255,1);
          border-top: 6px solid transparent;
          border-bottom: 6px solid transparent;
          font-size: 0;
          line-height: 0;
        }
      }
    }
    .index-block {
      position: absolute;
      top: 126px;
      left: 259px;
      .index-tags {
        position: absolute;
        left: 100px;
        top: 0px;
        background-color: rgba(255,255,255,1);
        border-radius: 5px;
        width: 250px;
        height: 30px;
        line-height: 30px;
        span {
          margin: 0 14px;
        }
      }
      .index-desc {
        position: absolute;
        left: 0px;
        top: 50px;
        width: 500px;
        padding: 20px 20px 10px 20px;
        border-radius: 2px;
        background-color: rgba(255,255,255,1);
        &.arrow-top::after {
          content: '';
          position: absolute;
          left: 190px;
          top: -6px;
          width: 0;
          height: 0;
          border-bottom: 6px solid rgba(255,255,255,1);
          border-left: 6px solid transparent;
          border-right: 6px solid transparent;
          font-size: 0;
          line-height: 0;
        }
      }
    }
    .build-block {
      position: absolute;
      top: 60px;
      right: 120px;
      .build-icon {
        position: absolute;
        right: 11px;
        top: 0px;
        background-color: rgba(255,255,255,1);
        border-radius: 5px;
        width: 30px;
        height: 30px;
        line-height: 30px;
        text-align: center;
      }
      .build-desc {
        position: absolute;
        right: 0px;
        top: 50px;
        width: 500px;
        padding: 20px 20px 10px 20px;
        border-radius: 2px;
        background-color: rgba(255,255,255,1);
        &.arrow-top::after {
          content: '';
          position: absolute;
          right: 20px;
          top: -6px;
          width: 0;
          height: 0;
          border-bottom: 6px solid rgba(255,255,255,1);
          border-left: 6px solid transparent;
          border-right: 6px solid transparent;
          font-size: 0;
          line-height: 0;
        }
      }
    }
    &.is-show-global-alter {
      .segment-block,
      .index-block {
        top: 310px;
      }
      .build-block {
        top: 260px;
      }
    }
    &.en-mode {
      .segment-block {
        .segment-tag {
          left: 85px;
        }
        .segment-desc {
          &.arrow-top::after {
            left: 120px;
          }
        }
      }
      .index-block {
        left: 160px;
        .index-tags {
          width: 360px;
          left: 185px;
        }
        .index-desc {
          left: 100px;
          &.arrow-top::after {
            left: 250px;
          }
        }
      }
    }
    &.brief-Menu {
      .segment-block {
        left: 75px;
        .segment-tag {
          left: 58px;
        }
        .segment-desc {
          &.arrow-top::after {
            left: 100px;
          }
        }
      }
      .index-block {
        left: 50px;
        .index-tags {
          width: 225px;
          left: 185px;
        }
        .index-desc {
          left: 100px;
          &.arrow-top::after {
            left: 180px;
          }
        }
      }
       &.en-mode {
         .segment-block {
           left: 78px;
           .segment-tag {
             left: 85px;
           }
           .segment-desc {
             &.arrow-top::after {
               left: 120px;
             }
           }
         }
         .index-block {
           left: 85px;
           .index-tags {
             width: 360px;
             left: 185px;
           }
           .index-desc {
             left: 100px;
             &.arrow-top::after {
               left: 250px;
             }
           }
         }
       }
    }
  }
}
</style>
