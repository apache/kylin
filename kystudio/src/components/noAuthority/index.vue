<template>
  <div class="user-no-authority">
    <div class="content">
      <template v-if="tipType === 'isNoAuthority'">
        <img src="../../assets/img/empty/empty_state_permission_denied.svg" alt="">
        <div class="text">
          <div class="desc no-auth">{{$t('noAuthorityText')}}</div>
          <div class="ksd-mb-16 desc">{{$t('noAuthorityText1', {time: jumpTimer})}}</div>
          <el-button @click="jumpToDashboard" plain>{{$t('dashboard')}}</el-button>
        </div>
      </template>
      <template v-else-if="tipType === 'isNotSemiAuto'">
        <i class="el-ksd-icon-sad_old"></i>
        <template v-if="$lang === 'en'">
          <p class="text"><span>{{$t('noModalAuthorityText1')}}</span><a href="javascript:void(0);" @click.self="jumpToDashboard" class="jump-address">{{$t('dashboard')}}</a><span>{{$t('noModalAuthorityText2', {time: jumpTimer})}}</span></p>
        </template>
        <template v-else>
          <p class="text"><span>{{$t('noModalAuthorityText1', {time: jumpTimer})}}</span><a href="javascript:void(0);" @click.self="jumpToDashboard" class="jump-address">{{$t('dashboard')}}</a></p>
        </template>
      </template>
      <template v-else>
        <img src="../../assets/img/empty/empty_state_404.svg" alt="404">
        <div class="text">
          <div class="desc no-auth">{{$t('is404Tip')}}</div>
          <div class="ksd-mb-16 desc">{{$t('is404Tip1', {time: jumpTimer})}}</div>
          <el-button @click="jumpToDashboard" plain>{{$t('dashboard')}}</el-button>
        </div>
      </template>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
// import { mapActions, mapGetters } from 'vuex'

@Component({
  beforeRouteEnter (to, from, next) {
    let type = ''
    if (to.query && Object.keys(to.query).length && 'resouce' in to.query) {
      type = to.query.resouce
    }
    next(vm => {
      vm.tipType = type
    })
  },
  locales: {
    'en': {
      noAuthorityText: 'Sorry, you don\'t have permission to access this page.',
      noAuthorityText1: 'Will automatically redirect to Homepage in {time} seconds.',
      noModalAuthorityText1: 'Recommendation mode is not supported in the current project. Please turn on the mode in Setting and try again. Will automatically redirect to ',
      noModalAuthorityText2: ' in {time} seconds.',
      is404Tip: 'Sorry, the page doesn\'t exist. ',
      is404Tip1: 'Will automatically redirect to Homepage in {time} seconds.',
      dashboard: 'Go to Homepage'
    }
  }
})
export default class NoAuthority extends Vue {
  jumpTimer = 5
  tipType = ''
  timer = null

  // 跳转至dashboard页面
  jumpToDashboard () {
    clearInterval(this.timer)
    this.$router.push('/query/insight')
  }

  mounted () {
    // 5秒后自动跳转至dashboard页面
    this.timer = setInterval(() => {
      this.jumpTimer -= 1
      if (this.jumpTimer === 0) {
        clearInterval(this.timer)
        if (this.$route.name !== 'noAuthority' && this.$route.name !== '404') return
        this.jumpToDashboard()
      }
    }, 1000)
  }
}
</script>
<style lang="less" scoped>
  @import "../../assets/styles/variables.less";
  .user-no-authority {
    width: 100%;
    height: 100%;
    text-align: center;
    position: relative;
    .content {
      position: absolute;
      top: 30%;
      left: 50%;
      transform: translate(-50%, -30%);
      .desc {
        font-size: 14px;
        line-height: 22px;
        text-align: center;
        color: @text-placeholder-color;
        max-width: 392px;
      }
      .no-auth {
        color: @text-normal-color;
      }
      .el-icon-ksd-lock {
        font-size: 50px;
        color: @text-placeholder-color;
      }
      .el-icon-ksd-sad {
        font-size: 50px;
      }
      .text {
        margin-top: 17px;
        font-size: 12px;
        color: @text-title-color;
      }
      .jump-address {
        color: @base-color;
      }
    }
  }
</style>
